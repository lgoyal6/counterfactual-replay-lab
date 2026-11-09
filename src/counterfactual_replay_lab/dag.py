"""Versioned dependency graphs for counterfactual replay.

A node binds its operation, operation version, canonical parameters, ordered
data inputs, control guards, effect kind and output schema. engine.py keys
cached results by these fields plus predecessor result hashes, so a node id
alone never identifies a reusable result.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import heapq
import json
import re
from typing import Any, Iterable

from .replay_state import canonical, digest

NODE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
EFFECT_KINDS = ("pure", "observation", "effect_intent")
MAX_NODES = 5000
MAX_EDGES = 50000
MAX_PARAM_BYTES = 4096
GRAPH_FORMAT = 1


class GraphError(ValueError):
    """The graph is malformed, cyclic or too large; nothing was executed."""


@dataclass(frozen=True)
class Guard:
    """Control edge: the node runs only if `node` is active and its output is in `allowed`."""
    node: str
    allowed: tuple[Any, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"node": self.node, "allowed": list(self.allowed)}

    def admits(self, value: Any) -> bool:
        encoded = canonical(value)
        return any(encoded == canonical(option) for option in self.allowed)


@dataclass(frozen=True)
class NodeSpec:
    id: str
    op: str
    op_version: str
    params: dict[str, Any] = field(default_factory=dict)
    inputs: tuple[str, ...] = ()
    guards: tuple[Guard, ...] = ()
    effect: str = "pure"
    output_schema: str = "any"

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "op": self.op, "op_version": self.op_version,
                "params": self.params, "inputs": list(self.inputs),
                "guards": [g.to_dict() for g in self.guards], "effect": self.effect,
                "output_schema": self.output_schema}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "NodeSpec":
        if not isinstance(value, dict):
            raise GraphError("Node must be an object")
        unknown = set(value) - {"id", "op", "op_version", "params", "inputs", "guards",
                                "effect", "output_schema"}
        if unknown:
            raise GraphError(f"Unknown node fields: {sorted(unknown)}")
        try:
            guards = tuple(Guard(str(g["node"]), tuple(g["allowed"])) for g in value.get("guards", ()))
            return cls(str(value["id"]), str(value["op"]), str(value["op_version"]),
                       dict(value.get("params", {})), tuple(str(x) for x in value.get("inputs", ())),
                       guards, str(value.get("effect", "pure")), str(value.get("output_schema", "any")))
        except (KeyError, TypeError) as error:
            raise GraphError(f"Malformed node: {error}") from error

    @property
    def spec_hash(self) -> str:
        return digest(self.to_dict())

    @property
    def predecessors(self) -> tuple[str, ...]:
        """Data inputs then guard nodes, deduplicated in declaration order."""
        seen: dict[str, None] = {}
        for name in self.inputs + tuple(g.node for g in self.guards):
            seen.setdefault(name, None)
        return tuple(seen)


class Graph:
    """Immutable validated DAG with indexed adjacency and a deterministic order.

    The canonical order is Kahn's algorithm with ties broken by node id, so two
    processes always schedule, compare and explain nodes identically.
    """

    __slots__ = ("env_version", "nodes", "order", "index", "successors", "spec_hashes",
                 "graph_hash", "_frozen")

    def __init__(self, nodes: Iterable[NodeSpec], env_version: str = "env-v1"):
        nodes = tuple(nodes)
        if not isinstance(env_version, str) or not env_version:
            raise GraphError("env_version is required")
        if not nodes:
            raise GraphError("Graph has no nodes")
        if len(nodes) > MAX_NODES:
            raise GraphError(f"Graph exceeds {MAX_NODES} nodes")
        by_id: dict[str, NodeSpec] = {}
        edges = 0
        for node in nodes:
            if not isinstance(node, NodeSpec):
                raise GraphError("Graph nodes must be NodeSpec values")
            if not NODE_ID.match(node.id):
                raise GraphError(f"Invalid node id {node.id!r}")
            if node.id in by_id:
                raise GraphError(f"Duplicate node id {node.id!r}")
            if node.effect not in EFFECT_KINDS:
                raise GraphError(f"Node {node.id} has unknown effect kind {node.effect!r}")
            try:
                encoded = canonical(node.params)
            except (TypeError, ValueError) as error:
                raise GraphError(f"Node {node.id} params are not canonical JSON: {error}") from error
            if len(encoded) > MAX_PARAM_BYTES:
                raise GraphError(f"Node {node.id} params exceed {MAX_PARAM_BYTES} bytes")
            if len(set(node.inputs)) != len(node.inputs):
                raise GraphError(f"Node {node.id} repeats a data input")
            for guard in node.guards:
                if not guard.allowed:
                    raise GraphError(f"Node {node.id} guard on {guard.node} admits nothing")
                try:
                    canonical(list(guard.allowed))
                except (TypeError, ValueError) as error:
                    raise GraphError(f"Node {node.id} guard values are not canonical: {error}") from error
            edges += len(node.predecessors)
            by_id[node.id] = node
        if edges > MAX_EDGES:
            raise GraphError(f"Graph exceeds {MAX_EDGES} edges")
        successors: dict[str, list[str]] = {name: [] for name in by_id}
        indegree = {name: 0 for name in by_id}
        for node in nodes:
            for pred in node.predecessors:
                if pred not in by_id:
                    raise GraphError(f"Node {node.id} depends on missing node {pred!r}")
                if pred == node.id:
                    raise GraphError(f"Cycle detected: {node.id} -> {node.id}")
                successors[pred].append(node.id)
                indegree[node.id] += 1
        ready = [name for name, degree in indegree.items() if degree == 0]
        heapq.heapify(ready)
        order: list[str] = []
        while ready:
            name = heapq.heappop(ready)
            order.append(name)
            for succ in successors[name]:
                indegree[succ] -= 1
                if indegree[succ] == 0:
                    heapq.heappush(ready, succ)
        if len(order) != len(by_id):
            raise GraphError("Cycle detected: " + " -> ".join(_find_cycle(by_id, set(by_id) - set(order))))
        object.__setattr__(self, "env_version", env_version)
        object.__setattr__(self, "nodes", by_id)
        object.__setattr__(self, "order", tuple(order))
        object.__setattr__(self, "index", {name: i for i, name in enumerate(order)})
        object.__setattr__(self, "successors", {k: tuple(sorted(v)) for k, v in successors.items()})
        object.__setattr__(self, "spec_hashes", {name: by_id[name].spec_hash for name in order})
        object.__setattr__(self, "graph_hash", digest({"format": GRAPH_FORMAT, "env_version": env_version,
                                                         "nodes": [self.spec_hashes[n] for n in sorted(by_id)]}))
        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name, value):
        raise AttributeError("Graph is immutable; build a new version instead")

    def verify_frozen(self) -> None:
        """Recompute identity so a mutated running snapshot fails closed."""
        current = digest({"format": GRAPH_FORMAT, "env_version": self.env_version,
                          "nodes": [self.nodes[n].spec_hash for n in sorted(self.nodes)]})
        if current != self.graph_hash:
            raise GraphError("Graph changed after it was frozen")

    def descendants(self, seeds: Iterable[str]) -> set[str]:
        """Seeds plus every data/control descendant: O(V_affected + E_affected)."""
        affected: set[str] = set()
        stack = [s for s in seeds if s in self.nodes]
        while stack:
            name = stack.pop()
            if name in affected:
                continue
            affected.add(name)
            stack.extend(s for s in self.successors[name] if s not in affected)
        return affected

    def to_dict(self) -> dict[str, Any]:
        return {"format": GRAPH_FORMAT, "env_version": self.env_version,
                "nodes": [self.nodes[n].to_dict() for n in self.order]}

    def to_json(self) -> str:
        return canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Graph":
        if not isinstance(value, dict) or value.get("format", GRAPH_FORMAT) != GRAPH_FORMAT:
            raise GraphError("Unsupported graph format")
        nodes = value.get("nodes")
        if not isinstance(nodes, list):
            raise GraphError("Graph nodes must be a list")
        return cls((NodeSpec.from_dict(n) for n in nodes), value.get("env_version", "env-v1"))

    @classmethod
    def from_json(cls, text: str) -> "Graph":
        try:
            return cls.from_dict(json.loads(text))
        except json.JSONDecodeError as error:
            raise GraphError(f"Graph is not JSON: {error}") from error

    def replace_nodes(self, *nodes: NodeSpec, remove: Iterable[str] = (), env_version: str | None = None) -> "Graph":
        """Return a new graph version with nodes replaced/added and others removed."""
        drop = set(remove)
        merged = {k: v for k, v in self.nodes.items() if k not in drop}
        for node in nodes:
            merged[node.id] = node
        return Graph(merged.values(), env_version or self.env_version)


def _find_cycle(nodes: dict[str, NodeSpec], remaining: set[str]) -> list[str]:
    """Return one cycle among nodes Kahn's algorithm could not order."""
    start = min(remaining)
    path, position = [start], {start: 0}
    current = start
    while True:
        nxt = min(p for p in nodes[current].predecessors if p in remaining)
        if nxt in position:
            cycle = path[position[nxt]:] + [nxt]
            return list(reversed(cycle))
        position[nxt] = len(path)
        path.append(nxt)
        current = nxt
