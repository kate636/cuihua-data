from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable


Edge = tuple[str, str]


def connected_components(edges: Iterable[Edge]) -> list[set[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for source, target in edges:
        graph[str(source)].add(str(target))
        graph[str(target)].add(str(source))
    result: list[set[str]] = []
    unseen = set(graph)
    while unseen:
        root = min(unseen)
        unseen.remove(root)
        component = {root}
        queue = [root]
        while queue:
            node = queue.pop()
            for neighbour in graph[node]:
                if neighbour in unseen:
                    unseen.remove(neighbour)
                    component.add(neighbour)
                    queue.append(neighbour)
        result.append(component)
    return sorted(result, key=lambda component: tuple(sorted(component)))


def topological_order(edges: Iterable[Edge]) -> list[str]:
    """Return a deterministic order and reject cycles used by formal flows."""
    outgoing: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = defaultdict(int)
    nodes: set[str] = set()
    for source, target in edges:
        source, target = str(source), str(target)
        nodes.update((source, target))
        if target not in outgoing[source]:
            outgoing[source].add(target)
            indegree[target] += 1
            indegree.setdefault(source, 0)
    ready = deque(sorted(node for node in nodes if indegree[node] == 0))
    ordered: list[str] = []
    while ready:
        node = ready.popleft()
        ordered.append(node)
        for target in sorted(outgoing[node]):
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)
    if len(ordered) != len(nodes):
        cycle_nodes = sorted(node for node in nodes if indegree[node] > 0)
        raise ValueError(f"relation graph contains a cycle: {cycle_nodes[:20]}")
    return ordered
