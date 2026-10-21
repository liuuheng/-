"""Project contracts runnable without a model or remote service."""
from collections import defaultdict

def route_customer_request(intent, slots, confirmed=False):
    if intent not in {"knowledge", "order_status", "cancel_order"}:
        return {"next": "clarify", "missing": ["intent"]}
    if intent == "knowledge":
        return {"next": "retrieve"}
    order_id = slots.get("order_id")
    if not isinstance(order_id, str) or not order_id.isdecimal():
        return {"next": "clarify", "missing": ["order_id"]}
    if intent == "cancel_order" and not confirmed:
        return {"next": "confirm", "order_id": order_id}
    # The execution service must still enforce identity, ownership and idempotency.
    return {"next": "execute", "tool": intent, "order_id": order_id}

def aggregate_product_hits(hits, top_k=5):
    """hits: iterable of {product_id, image_id, score}; higher score is better."""
    best = {}
    for hit in hits:
        key = hit["product_id"]
        if key not in best or hit["score"] > best[key]["score"]:
            best[key] = dict(hit)
    return sorted(best.values(), key=lambda h: (-h["score"], h["product_id"]))[:top_k]

def check_citations(claims, retrieved_source_ids):
    """Validate reference membership only; this does not verify factual support."""
    allowed = set(retrieved_source_ids)
    problems = []
    for index, claim in enumerate(claims):
        refs = claim.get("source_ids", [])
        if not refs:
            problems.append((index, "missing citation"))
        for ref in refs:
            if ref not in allowed:
                problems.append((index, "unknown source: " + ref))
    return problems

def validate_prerequisites(edges):
    """(A, B) means A must be learned before B; raise on cycles."""
    adjacency = defaultdict(list)
    for a, b in edges:
        adjacency[a].append(b)
    visiting, finished = set(), set()
    def visit(node):
        if node in visiting:
            raise ValueError("cyclic prerequisite: " + node)
        if node in finished:
            return
        visiting.add(node)
        for child in adjacency.get(node, []):
            visit(child)
        visiting.remove(node)
        finished.add(node)
    for node in list(adjacency):
        visit(node)
    return True

def select_visible_chunks(chunks, tenant_id, user_groups, active_versions):
    """Demo ACL contract; identity/groups must come from the trusted server."""
    groups = set(user_groups)
    return [dict(c) for c in chunks
            if c["tenant_id"] == tenant_id
            and c["version"] == active_versions.get(c["doc_id"])
            and bool(groups & set(c["allowed_groups"]))]
