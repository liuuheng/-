"""Small CPU examples for the interview notes; no models, keys or external services."""
import asyncio
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path
import numpy as np

def make_counter():
    value = 0
    def increment():
        nonlocal value
        value += 1
        return value
    return increment

def two_sum(values, target):
    seen = {}
    for index, value in enumerate(values):
        if target - value in seen:
            return seen[target - value], index
        seen[value] = index
    return None

def binary_metrics(truth, prediction):
    if len(truth) != len(prediction) or not truth:
        raise ValueError("non-empty equal-length inputs required")
    tp = sum(y == 1 and p == 1 for y, p in zip(truth, prediction))
    fp = sum(y == 0 and p == 1 for y, p in zip(truth, prediction))
    fn = sum(y == 1 and p == 0 for y, p in zip(truth, prediction))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}

def standardize_from_train(train, test):
    mean = train.mean(axis=0)
    scale = train.std(axis=0)
    scale = np.where(scale == 0, 1.0, scale)
    return (train - mean) / scale, (test - mean) / scale

def least_squares(x, y):
    design = np.column_stack([np.ones(len(x)), x])
    # lstsq also handles rank-deficient matrices; avoid an explicit matrix inverse.
    return np.linalg.lstsq(design, y, rcond=None)[0]

def entropy(labels):
    _, counts = np.unique(labels, return_counts=True)
    if not len(counts):
        return 0.0
    p = counts / counts.sum()
    return float(-(p * np.log2(p)).sum())

def fit_pca(train, components=2):
    mean = train.mean(axis=0)
    _, singular_values, vt = np.linalg.svd(train - mean, full_matrices=False)
    basis = vt[:components].T
    return mean, basis, singular_values

def softmax(logits):
    maximum = np.max(logits, axis=-1, keepdims=True)
    if not np.all(np.isfinite(maximum)):
        raise ValueError("every row must have an unmasked finite value")
    weights = np.exp(logits - maximum)
    return weights / weights.sum(axis=-1, keepdims=True)

def causal_attention(q, k, v):
    if q.shape != k.shape or len(k) != len(v):
        raise ValueError("this teaching example uses equal-length Q and K")
    scores = q @ k.T / np.sqrt(q.shape[-1])
    causal = np.tril(np.ones(scores.shape, dtype=bool))
    weights = softmax(np.where(causal, scores, -np.inf))
    return weights @ v, weights

def layer_norm(x, eps=1e-6):
    mean = x.mean(axis=-1, keepdims=True)
    var = ((x - mean) ** 2).mean(axis=-1, keepdims=True)
    return (x - mean) / np.sqrt(var + eps)

def rms_norm(x, eps=1e-6):
    return x / np.sqrt((x * x).mean(axis=-1, keepdims=True) + eps)

def rope(x, position, theta=10000.0):
    if x.shape[-1] % 2:
        raise ValueError("head dimension must be even")
    frequency = theta ** (-np.arange(0, x.shape[-1], 2) / x.shape[-1])
    angle = position * frequency
    out = np.empty_like(x, dtype=float)
    a, b = x[..., 0::2], x[..., 1::2]
    out[..., 0::2] = a * np.cos(angle) - b * np.sin(angle)
    out[..., 1::2] = a * np.sin(angle) + b * np.cos(angle)
    return out

def lora_forward(x, weight, a, b, alpha):
    rank = a.shape[0]
    return x @ weight.T + (alpha / rank) * ((x @ a.T) @ b.T)

def kv_cache_bytes(batch, layers, tokens, kv_heads, head_dim, bytes_per_element=2):
    return 2 * batch * layers * tokens * kv_heads * head_dim * bytes_per_element

def causal_lm_loss(logits, labels):
    # Input shape: [batch, tokens, vocab]; prompt/pad labels are already -100.
    shifted_logits, shifted_labels = logits[:, :-1, :], labels[:, 1:]
    valid = shifted_labels != -100
    if not valid.any():
        raise ValueError("no supervised response tokens")
    selected_logits, targets = shifted_logits[valid], shifted_labels[valid]
    log_z = np.logaddexp.reduce(selected_logits, axis=-1)
    selected = selected_logits[np.arange(len(targets)), targets]
    return float((log_z - selected).mean())

def dpo_loss(chosen_logp, rejected_logp, ref_chosen_logp, ref_rejected_logp, beta=0.1):
    margin = beta * ((chosen_logp - rejected_logp) - (ref_chosen_logp - ref_rejected_logp))
    return float(np.logaddexp(0.0, -margin).mean())

def grpo_advantages(rewards, eps=1e-8):
    rewards = np.asarray(rewards, dtype=float)
    if rewards.ndim != 2:
        raise ValueError("shape must be [prompts, completions_per_prompt]")
    return (rewards - rewards.mean(axis=1, keepdims=True)) / (rewards.std(axis=1, keepdims=True) + eps)

def ppo_surrogate(ratio, advantage, epsilon=0.2):
    unclipped = ratio * advantage
    clipped = np.clip(ratio, 1 - epsilon, 1 + epsilon) * advantage
    return float(np.minimum(unclipped, clipped).mean())

def cosine_similarity(query, vectors):
    query = np.asarray(query, dtype=float)
    vectors = np.asarray(vectors, dtype=float)
    denom = np.linalg.norm(query) * np.linalg.norm(vectors, axis=1)
    return np.divide(vectors @ query, denom, out=np.zeros(len(vectors)), where=denom != 0)

def rrf_fuse(rankings, constant=60):
    if constant <= 0:
        raise ValueError("constant must be positive")
    scores = defaultdict(float)
    for ranking in rankings:
        seen = set()
        for rank, doc_id in enumerate(ranking, start=1):
            if doc_id in seen:
                continue
            seen.add(doc_id)
            scores[doc_id] += 1 / (constant + rank)
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))

def retrieval_metrics(retrieved, relevant, k=5):
    if not relevant:
        raise ValueError("no-answer cases must be evaluated separately")
    top = list(dict.fromkeys(retrieved))[:k]
    hit = set(top) & set(relevant)
    reciprocal_rank = next((1 / rank for rank, doc in enumerate(top, 1) if doc in relevant), 0.0)
    return {"recall_at_k": len(hit) / len(set(relevant)), "mrr": reciprocal_rank}

def chunk_with_offsets(text, size=100, overlap=20):
    if not 0 <= overlap < size:
        raise ValueError("require 0 <= overlap < size")
    result = []
    start = 0
    while start < len(text):
        end = min(len(text), start + size)
        result.append({"start": start, "end": end, "text": text[start:end]})
        if end == len(text):
            break
        start = end - overlap
    return result

def decode_bio(tokens, labels):
    if len(tokens) != len(labels):
        raise ValueError("tokens and labels must align")
    result, active, start = [], None, None
    for i, label in enumerate(labels + ["O"]):
        prefix, _, kind = label.partition("-")
        if active is not None and not (prefix == "I" and kind == active):
            result.append((active, start, i, "".join(tokens[start:i])))
            active, start = None, None
        if prefix == "B":
            active, start = kind, i
        elif prefix == "I" and active != kind:
            raise ValueError("invalid BIO transition")
    return result

def validate_tool_call(payload):
    data = json.loads(payload)
    if not isinstance(data, dict) or set(data) != {"tool", "args"}:
        raise ValueError("invalid envelope")
    if data["tool"] != "get_order" or not isinstance(data["args"], dict):
        raise ValueError("tool not allowed")
    order_id = data["args"].get("order_id")
    if set(data["args"]) != {"order_id"} or not isinstance(order_id, str) or not order_id.isdecimal():
        raise ValueError("invalid order_id")
    return data

async def bounded_map(items, operation, concurrency=4, timeout=1.0):
    semaphore = asyncio.Semaphore(concurrency)
    async def one(item):
        async with semaphore:
            return await asyncio.wait_for(operation(item), timeout=timeout)
    return await asyncio.gather(*(one(item) for item in items))

def sse_event(event, data, event_id):
    if any(c in event for c in "\r\n") or any(c in str(event_id) for c in "\r\n"):
        raise ValueError("event/id must be single-line")
    body = json.dumps(data, ensure_ascii=False)
    return f"id: {event_id}\nevent: {event}\ndata: {body}\n\n"

def readonly_query(db_path, sql, max_rows=100):
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        allowed_actions = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
        allowed_functions = {"count", "sum", "avg", "min", "max", "abs", "round", "lower", "upper", "length"}
        def authorizer(action, arg1, arg2, database, source):
            if action not in allowed_actions:
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_FUNCTION and (arg2 or "").lower() not in allowed_functions:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        connection.set_authorizer(authorizer)
        return connection.execute(sql).fetchmany(max_rows)
    finally:
        connection.close()

def clip_contrastive_loss(image_features, text_features, logit_scale=1.0):
    def normalize(x):
        return x / np.maximum(np.linalg.norm(x, axis=1, keepdims=True), 1e-12)
    logits = logit_scale * normalize(image_features) @ normalize(text_features).T
    image_logp = logits - np.logaddexp.reduce(logits, axis=1, keepdims=True)
    text_logp = logits.T - np.logaddexp.reduce(logits.T, axis=1, keepdims=True)
    return float(-(np.diag(image_logp).mean() + np.diag(text_logp).mean()) / 2)

def diffusion_forward(x0, noise, alpha_bar):
    if not 0 <= alpha_bar <= 1:
        raise ValueError("alpha_bar must be in [0,1]")
    return np.sqrt(alpha_bar) * x0 + np.sqrt(1 - alpha_bar) * noise
