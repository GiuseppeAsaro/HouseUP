#!/usr/bin/env python3
import argparse
import csv
import os
import sys
import time
import uuid
import zlib
from collections import OrderedDict, defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    ConnectionFailure,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.core.security import hash_password


DEFAULT_SEED_SELLER_COUNT = 20
DEFAULT_SEED_BUYER_COUNT = 30
DEFAULT_SELLER_TARGET_HOUSES = 15
DEFAULT_BUYER_MAX_BOUGHT_HOUSES = 5
DEFAULT_BATCH_SIZE = 10000
DEFAULT_SEED_PASSWORD = "houseup-seed-password"
DEFAULT_CSV_PATH = Path(__file__).resolve().parent / "realtor-data.zip.csv"
DEFAULT_MONGO_DB = "houseup"
DEFAULT_WRITE_THREADS = 4
DEFAULT_MAX_INFLIGHT_BATCHES = 12
DEFAULT_USER_FLUSH_ITEMS = 100000
DEFAULT_PROGRESS_EVERY = 10000
DEFAULT_BOOKING_RESULTS_MAX = 60000
DEFAULT_FEEDBACK_MAX = 300
DEFAULT_MAX_FEEDBACK_PER_USER = DEFAULT_BUYER_MAX_BOUGHT_HOUSES
DEFAULT_FEEDBACK_USER_SHARE = 0.4
DEFAULT_FEEDBACK_SELECTION_RATE = 0.35
DEFAULT_FEEDBACK_MIN_GAP_HOURS = 36
DEFAULT_FEEDBACK_GAP_JITTER_MAX_HOURS = 48
DEFAULT_SOLD_BOOKING_SELECTION_RATE = 0.15
DEFAULT_ACTIVE_BOOKING_SELECTION_RATE = 0.05
DEFAULT_BOOKING_PURCHASE_RATE = 0.25
# Synthetic negotiation keeps purchase final price at or below listing price.
PURCHASE_FINAL_PRICE_MIN_RATIO = 0.90
PURCHASE_FINAL_PRICE_MAX_RATIO = 1.00
DEFAULT_BOOKING_INSERT_BATCH_SIZE = 5000
DEFAULT_FEEDBACK_INSERT_BATCH_SIZE = 5000
WRITE_RETRY_ATTEMPTS = 8
WRITE_RETRY_BASE_SLEEP = 0.5
U32_MAX = 0xFFFFFFFF
BOOKING_SCAN_SAFETY_MULTIPLIER = 1.25
BOOKING_SCAN_MIN_ROWS = 5000
RATIO_CACHE_MAX_ITEMS = 750000
DEFAULT_USER_SET_BATCH_SIZE = 1000

PHASE_ALL = "all"
PHASE_HOUSES_USERS = "houses-users"
PHASE_ANALYTICS = "analytics"
RESET_DELETE = "delete"
RESET_DROP = "drop"
RESET_NONE = "none"
NOT_WRITABLE_PRIMARY_CODES = {10107, 13435, 13436}
RETRYABLE_CODES = {91, 11600, 11602, 13435, 13436, 189, 10107}

POSITIVE_FEEDBACK_COMMENTS = (
    "Friendly process and clear follow-up.",
    "Helpful seller, visit met expectations.",
    "Good experience overall.",
    "Communication was fast and useful.",
)
NEUTRAL_FEEDBACK_COMMENTS = (
    "Average experience, nothing special.",
    "Visit was okay but could be smoother.",
    "Service was acceptable.",
    "Everything worked, but with minor delays.",
)
NEGATIVE_FEEDBACK_COMMENTS = (
    "Poor communication and unclear timing.",
    "Visit management was disappointing.",
    "Support was slow and not very helpful.",
    "Experience below expectations.",
)


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    value_str = str(value).strip()
    if value_str == "":
        return None
    try:
        return float(value_str)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    value_str = str(value).strip()
    if value_str == "":
        return None
    try:
        return int(float(value_str))
    except (TypeError, ValueError):
        return None


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str if value_str else None


def _house_is_sold_from_doc(house_doc: Dict[str, Any]) -> bool:
    raw_is_sold = house_doc.get("is_sold")
    if isinstance(raw_is_sold, bool):
        return raw_is_sold
    if isinstance(raw_is_sold, (int, float)):
        return bool(raw_is_sold)
    if isinstance(raw_is_sold, str):
        is_sold_norm = raw_is_sold.strip().lower()
        if is_sold_norm in {"true", "1", "yes", "y", "sold"}:
            return True
        if is_sold_norm in {"false", "0", "no", "n", "for_sale", "for sale", "active"}:
            return False

    return False


def _ceil_div(value: int, divisor: int) -> int:
    safe_divisor = max(int(divisor), 1)
    safe_value = max(int(value), 0)
    return (safe_value + safe_divisor - 1) // safe_divisor


def _compute_dataset_counts(csv_path: Path, row_limit: int = 0) -> Tuple[int, int, int]:
    total_rows = 0
    sold_rows = 0
    active_rows = 0
    with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row_number, row in enumerate(reader, start=1):
            if row_limit and row_number > row_limit:
                break
            total_rows += 1
            is_sold = str(row.get("status", "")).strip().lower() == "sold"
            if is_sold:
                sold_rows += 1
            else:
                active_rows += 1
    return total_rows, sold_rows, active_rows


def _derive_seed_user_counts(
    *,
    total_rows: int,
    sold_rows: int,
    seller_count_override: int,
    buyer_count_override: int,
) -> Tuple[int, int]:
    if seller_count_override > 0:
        seller_count = seller_count_override
    else:
        seller_count = max(
            1,
            _ceil_div(max(total_rows, 1), DEFAULT_SELLER_TARGET_HOUSES),
        )

    if buyer_count_override > 0:
        buyer_count = buyer_count_override
    else:
        buyer_count = max(
            1,
            _ceil_div(max(sold_rows, 1), DEFAULT_BUYER_MAX_BOUGHT_HOUSES),
        )

    return seller_count, buyer_count


def _is_retryable_write_error(exc: Exception) -> bool:
    if isinstance(exc, (AutoReconnect, NotPrimaryError, ConnectionFailure, NetworkTimeout)):
        return True

    if isinstance(exc, OperationFailure):
        if exc.code in RETRYABLE_CODES:
            return True
        if "retryable" in str(exc).lower() or "notprimary" in str(exc).lower():
            return True

    return False


def _is_not_writable_primary_error(exc: Exception) -> bool:
    if isinstance(exc, NotPrimaryError):
        return True
    if isinstance(exc, OperationFailure):
        if exc.code in NOT_WRITABLE_PRIMARY_CODES:
            return True
        message = str(exc).lower()
        if "not writable primary" in message or "not primary" in message:
            return True
    return False


def _ensure_writable_primary(mongo_client: MongoClient) -> None:
    try:
        hello = mongo_client.admin.command("hello")
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB hello preflight failed: {exc}") from exc

    is_primary = bool(hello.get("isWritablePrimary") or hello.get("ismaster"))
    if is_primary:
        return

    me = hello.get("me")
    primary = hello.get("primary")
    set_name = hello.get("setName")
    raise RuntimeError(
        "MongoDB node is not writable primary. "
        f"set={set_name} me={me} primary={primary}"
    )


def _run_with_retry(fn: Callable[[], Any], op_name: str) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(1, WRITE_RETRY_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if _is_not_writable_primary_error(exc):
                raise RuntimeError(
                    f"{op_name} aborted: MongoDB node is not writable primary."
                ) from exc
            last_exc = exc
            if not _is_retryable_write_error(exc) or attempt == WRITE_RETRY_ATTEMPTS:
                raise
            sleep_s = WRITE_RETRY_BASE_SLEEP * (2 ** (attempt - 1))
            print(
                f"[retry] {op_name} failed (attempt {attempt}/{WRITE_RETRY_ATTEMPTS}): "
                f"{type(exc).__name__}; sleeping {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Unexpected retry loop exit for operation: {op_name}")


def _mix_u32(value: int) -> int:
    value &= U32_MAX
    value ^= (value >> 16)
    value = (value * 0x7FEB352D) & U32_MAX
    value ^= (value >> 15)
    value = (value * 0x846CA68B) & U32_MAX
    value ^= (value >> 16)
    return value & U32_MAX


def _stable_ratio_hash_u32(value: str) -> int:
    # Two CRC passes + avalanche mix break linear prefix correlations while
    # staying deterministic and much faster than cryptographic hashes.
    encoded = value.encode("utf-8")
    h1 = zlib.crc32(encoded) & U32_MAX
    h2 = zlib.crc32(encoded, 0x9E3779B9) & U32_MAX
    mixed = h1 ^ ((h2 << 1) & U32_MAX) ^ (h2 >> 1)
    return _mix_u32(mixed)


def _build_seed_users(
    seed_password: str,
    seller_count: int,
    buyer_count: int,
) -> Tuple[
    List[Dict[str, Any]],
    Dict[int, Dict[str, Any]],
    List[ObjectId],
    List[ObjectId],
    Dict[ObjectId, Dict[str, Any]],
    List[str],
]:
    seed_password_hash = hash_password(seed_password)
    users: List[Dict[str, Any]] = []
    seller_embeds: Dict[int, Dict[str, Any]] = {}
    seller_oids: List[ObjectId] = []
    buyer_oids: List[ObjectId] = []
    buyer_embeds: Dict[ObjectId, Dict[str, Any]] = {}
    seed_emails: List[str] = []
    seller_digits = max(len(str(max(int(seller_count), 1))), 2)
    buyer_digits = max(len(str(max(int(buyer_count), 1))), 2)

    for index in range(1, max(int(seller_count), 1) + 1):
        seller_oid = ObjectId()
        full_name = f"Seller {index:0{seller_digits}d}"
        email = f"seller_{index:0{seller_digits}d}@houseup.local"
        user_doc = {
            "_id": seller_oid,
            "email": email,
            "role": "seller",
            "full_name": full_name,
            "city": "Realtor City",
            "phone": f"+1000000{index:07d}",
            "password_hash": seed_password_hash,
        }
        users.append(user_doc)
        seed_emails.append(email)
        seller_oids.append(seller_oid)
        seller_embeds[index - 1] = {
            "id": seller_oid,
            "full_name": full_name,
            "email": email,
            "role": "seller",
            "phone": user_doc["phone"],
            "city": user_doc["city"],
        }

    for index in range(1, max(int(buyer_count), 1) + 1):
        buyer_oid = ObjectId()
        full_name = f"Buyer {index:0{buyer_digits}d}"
        email = f"buyer_{index:0{buyer_digits}d}@houseup.local"
        user_doc = {
            "_id": buyer_oid,
            "email": email,
            "role": "buyer",
            "full_name": full_name,
            "city": "Realtor City",
            "phone": f"+2000000{index:07d}",
            "password_hash": seed_password_hash,
        }
        users.append(user_doc)
        seed_emails.append(email)
        buyer_oids.append(buyer_oid)
        buyer_embeds[buyer_oid] = {
            "id": buyer_oid,
            "full_name": full_name,
            "email": email,
            "phone": user_doc["phone"],
        }

    admin_oid = ObjectId()
    admin_email = "admin_01@houseup.local"
    admin_doc = {
        "_id": admin_oid,
        "email": admin_email,
        "role": "admin",
        "full_name": "Admin 01",
        "city": "Realtor City",
        "phone": "+30000000001",
        "password_hash": seed_password_hash,
    }
    users.append(admin_doc)
    seed_emails.append(admin_email)

    return users, seller_embeds, seller_oids, buyer_oids, buyer_embeds, seed_emails


def _write_houses_batch(houses_collection: Any, house_docs: List[Dict[str, Any]]) -> None:
    if not house_docs:
        return

    def _op() -> Any:
        try:
            return houses_collection.insert_many(house_docs, ordered=False)
        except BulkWriteError as exc:
            write_errors = exc.details.get("writeErrors", [])
            if write_errors and all(err.get("code") == 11000 for err in write_errors):
                # Safe on retry after uncertain commit outcome.
                return None
            raise

    _run_with_retry(_op, "houses.bulk_write")


def _flush_houses(
    houses_collection: Any,
    house_docs_pending: List[Dict[str, Any]],
    house_executor: Optional[ThreadPoolExecutor],
    house_futures: List[Future],
    max_inflight_batches: int,
) -> None:
    if not house_docs_pending:
        return

    docs = list(house_docs_pending)
    house_docs_pending.clear()

    if house_executor is None:
        _write_houses_batch(houses_collection, docs)
        return

    house_futures.append(
        house_executor.submit(_write_houses_batch, houses_collection, docs)
    )

    if len(house_futures) >= max_inflight_batches:
        done, pending = wait(house_futures, return_when=FIRST_COMPLETED)
        for future in done:
            future.result()
        house_futures[:] = list(pending)


def _drain_house_futures(house_futures: List[Future]) -> None:
    if not house_futures:
        return

    done, _ = wait(house_futures)
    for future in done:
        future.result()
    house_futures.clear()


def _prepend_unique_id(raw_ids: OrderedDict[str, None], raw_id: Any) -> None:
    value = str(raw_id).strip()
    if not value:
        return

    if value in raw_ids:
        raw_ids.move_to_end(value, last=False)
        return
    raw_ids[value] = None
    raw_ids.move_to_end(value, last=False)


class _HouseListState:
    __slots__ = ("order", "embed_by_id")

    def __init__(self) -> None:
        self.order: OrderedDict[str, None] = OrderedDict()
        self.embed_by_id: Dict[str, Dict[str, Any]] = {}

    def promote(self, embed: Dict[str, Any]) -> None:
        raw_embed_id = embed.get("id")
        if raw_embed_id is None:
            return
        embed_id = str(raw_embed_id).strip()
        if not embed_id:
            return

        self.embed_by_id[embed_id] = embed
        _prepend_unique_id(self.order, embed_id)

    def export(self) -> List[Dict[str, Any]]:
        embeds: List[Dict[str, Any]] = []
        for house_id in self.order:
            embed = self.embed_by_id.get(house_id)
            if embed is not None:
                embeds.append(embed)
        return embeds


def _get_or_create_house_state(
    states_by_user: Dict[ObjectId, _HouseListState],
    owner_id: ObjectId,
) -> _HouseListState:
    state = states_by_user.get(owner_id)
    if state is None:
        state = _HouseListState()
        states_by_user[owner_id] = state
    return state


def _promote_latest_house_embed(
    states_by_user: Dict[ObjectId, _HouseListState],
    owner_id: ObjectId,
    embed: Dict[str, Any],
) -> None:
    state = _get_or_create_house_state(states_by_user, owner_id)
    state.promote(embed)


def _coerce_object_id(raw_id: Any) -> Optional[ObjectId]:
    if isinstance(raw_id, ObjectId):
        return raw_id
    if isinstance(raw_id, str):
        value = raw_id.strip()
        if value and ObjectId.is_valid(value):
            return ObjectId(value)
    return None


def _bulk_write_update_ops_in_chunks(
    users_collection: Any,
    user_ops: List[UpdateOne],
    op_name: str,
    batch_size: int = DEFAULT_USER_SET_BATCH_SIZE,
) -> None:
    if not user_ops:
        return
    safe_batch = max(int(batch_size), 1)
    for start in range(0, len(user_ops), safe_batch):
        chunk = user_ops[start : start + safe_batch]
        _run_with_retry(
            lambda chunk=chunk: users_collection.bulk_write(chunk, ordered=False),
            f"{op_name}.bulk_write[{start}:{start + len(chunk)}]",
        )


def _flush_user_updates(
    users_collection: Any,
    seller_for_sale_states: Dict[ObjectId, _HouseListState],
    seller_sold_states: Dict[ObjectId, _HouseListState],
    buyer_bought_states: Dict[ObjectId, _HouseListState],
) -> int:
    user_ops: List[UpdateOne] = []
    flushed_items = 0

    seller_oids = set(seller_for_sale_states.keys()) | set(seller_sold_states.keys())
    for seller_oid in seller_oids:
        push_payload: Dict[str, Any] = {}
        for_sale_state = seller_for_sale_states.get(seller_oid)
        if for_sale_state is not None:
            for_sale_houses = for_sale_state.export()
            if for_sale_houses:
                push_payload["for_sale_houses"] = {
                    "$each": for_sale_houses,
                    "$position": 0,
                }
                flushed_items += len(for_sale_houses)

        sold_state = seller_sold_states.get(seller_oid)
        if sold_state is not None:
            sold_houses = sold_state.export()
            if sold_houses:
                push_payload["sold_houses"] = {
                    "$each": sold_houses,
                    "$position": 0,
                }
                flushed_items += len(sold_houses)

        if push_payload:
            user_ops.append(
                UpdateOne(
                    {"_id": seller_oid},
                    {"$push": push_payload},
                    upsert=False,
                )
            )

    for buyer_oid, bought_state in buyer_bought_states.items():
        bought_houses = bought_state.export()
        if not bought_houses:
            continue

        push_payload: Dict[str, Any] = {}
        if bought_houses:
            push_payload["bought_houses"] = {
                "$each": bought_houses,
                "$position": 0,
            }
            flushed_items += len(bought_houses)
        user_ops.append(UpdateOne({"_id": buyer_oid}, {"$push": push_payload}, upsert=False))

    _bulk_write_update_ops_in_chunks(users_collection, user_ops, op_name="users")

    seller_for_sale_states.clear()
    seller_sold_states.clear()
    buyer_bought_states.clear()
    return flushed_items


def _rebuild_user_house_lists_post_pass(
    houses_collection: Any,
    users_collection: Any,
    buyer_oids: List[ObjectId],
) -> int:
    seller_for_sale_states: Dict[ObjectId, _HouseListState] = {}
    seller_sold_states: Dict[ObjectId, _HouseListState] = {}
    buyer_bought_states: Dict[ObjectId, _HouseListState] = {}
    sold_assignment_cursor = 0

    house_projection = {
        "_id": 1,
        "is_sold": 1,
        "city": 1,
        "zip_code": 1,
        "for_sale_by.id": 1,
    }

    for house_doc in houses_collection.find({}, house_projection).sort("_id", 1):
        house_oid = house_doc.get("_id")
        if not isinstance(house_oid, ObjectId):
            continue

        house_embed: Dict[str, Any] = {"id": str(house_oid)}

        city = house_doc.get("city")
        if city is not None:
            city_value = str(city).strip()
            if city_value:
                house_embed["city"] = city_value

        zip_code = house_doc.get("zip_code")
        if zip_code is not None:
            zip_code_value = str(zip_code).strip()
            if zip_code_value:
                house_embed["zip_code"] = zip_code_value

        for_sale_by = house_doc.get("for_sale_by")
        seller_oid = None
        if isinstance(for_sale_by, dict):
            seller_oid = _coerce_object_id(for_sale_by.get("id"))
        if seller_oid is None:
            continue

        is_sold = _house_is_sold_from_doc(house_doc)
        if is_sold:
            _promote_latest_house_embed(
                seller_sold_states,
                seller_oid,
                house_embed,
            )
        else:
            _promote_latest_house_embed(
                seller_for_sale_states,
                seller_oid,
                house_embed,
            )
        if is_sold and buyer_oids:
            buyer_oid = buyer_oids[sold_assignment_cursor % len(buyer_oids)]
            _promote_latest_house_embed(
                buyer_bought_states,
                buyer_oid,
                house_embed,
            )
            sold_assignment_cursor += 1
    user_ops: List[UpdateOne] = []
    seller_oids = set(seller_for_sale_states.keys()) | set(seller_sold_states.keys())
    for seller_oid in seller_oids:
        set_payload: Dict[str, Any] = {}
        unset_payload: Dict[str, str] = {}

        for_sale_state = seller_for_sale_states.get(seller_oid)
        for_sale_houses = [] if for_sale_state is None else for_sale_state.export()
        if for_sale_houses:
            set_payload["for_sale_houses"] = for_sale_houses
        else:
            unset_payload["for_sale_houses"] = ""

        sold_state = seller_sold_states.get(seller_oid)
        sold_houses = [] if sold_state is None else sold_state.export()
        if sold_houses:
            set_payload["sold_houses"] = sold_houses
        else:
            unset_payload["sold_houses"] = ""
        update_payload: Dict[str, Any] = {}
        if set_payload:
            update_payload["$set"] = set_payload
        if unset_payload:
            update_payload["$unset"] = unset_payload
        if update_payload:
            user_ops.append(UpdateOne({"_id": seller_oid}, update_payload, upsert=False))

    for buyer_oid in buyer_oids:
        bought_state = buyer_bought_states.get(buyer_oid)
        bought_houses = [] if bought_state is None else bought_state.export()

        set_payload: Dict[str, Any] = {}
        unset_payload: Dict[str, str] = {}
        if bought_houses:
            set_payload["bought_houses"] = bought_houses
        else:
            unset_payload["bought_houses"] = ""

        update_payload: Dict[str, Any] = {}
        if set_payload:
            update_payload["$set"] = set_payload
        if unset_payload:
            update_payload["$unset"] = unset_payload
        if update_payload:
            user_ops.append(UpdateOne({"_id": buyer_oid}, update_payload, upsert=False))

    _bulk_write_update_ops_in_chunks(
        users_collection=users_collection,
        user_ops=user_ops,
        op_name="users.postpass",
    )
    return sold_assignment_cursor


_ratio_cache: Dict[str, float] = {}

def _deterministic_ratio(*parts: Any) -> float:
    key = "|".join(str(part) for part in parts)
    cached = _ratio_cache.get(key)
    if cached is not None:
        return cached

    value = _stable_ratio_hash_u32(key) / U32_MAX
    if len(_ratio_cache) >= RATIO_CACHE_MAX_ITEMS:
        _ratio_cache.clear()
    _ratio_cache[key] = value
    return value


def _deterministic_feedback_rating(
    house_bought: bool,
    house_id: str,
    ratio_getter: Optional[Callable[[str, str], float]] = None,
) -> int:
    ratio_fn = ratio_getter or _deterministic_ratio
    ratio = ratio_fn("feedback_rating", house_id)
    if house_bought:
        if ratio < 0.45:
            return 5
        if ratio < 0.78:
            return 4
        if ratio < 0.92:
            return 3
        if ratio < 0.97:
            return 2
        return 1

    if ratio < 0.06:
        return 5
    if ratio < 0.22:
        return 4
    if ratio < 0.50:
        return 3
    if ratio < 0.80:
        return 2
    return 1


def _deterministic_feedback_comment(
    rating: int,
    house_id: str,
    ratio_getter: Optional[Callable[[str, str], float]] = None,
) -> Optional[str]:
    ratio_fn = ratio_getter or _deterministic_ratio
    include_ratio = ratio_fn("feedback_comment_include", house_id)
    include_comment = False
    if rating <= 2:
        include_comment = include_ratio < 0.55
    elif rating == 3:
        include_comment = include_ratio < 0.22
    else:
        include_comment = include_ratio < 0.30

    if not include_comment:
        return None

    if rating <= 2:
        pool = NEGATIVE_FEEDBACK_COMMENTS
    elif rating == 3:
        pool = NEUTRAL_FEEDBACK_COMMENTS
    else:
        pool = POSITIVE_FEEDBACK_COMMENTS

    text_ratio = ratio_fn("feedback_comment_text", house_id)
    index = min(int(text_ratio * len(pool)), len(pool) - 1)
    return pool[index]


def _insert_many_in_batches(
    collection: Any,
    docs: List[Dict[str, Any]],
    batch_size: int,
    op_name: str,
) -> None:
    if not docs:
        return

    safe_batch = max(int(batch_size), 1)
    for start in range(0, len(docs), safe_batch):
        chunk = docs[start : start + safe_batch]
        _run_with_retry(
            lambda chunk=chunk: collection.insert_many(chunk, ordered=False),
            f"{op_name}.insert_many[{start}:{start + len(chunk)}]",
        )


def _build_booking_result_house_embed(
    house_id: str,
    price: Any,
    city: Any = None,
) -> Dict[str, Any]:
    embed: Dict[str, Any] = {"id": str(house_id)}
    parsed_price = _parse_float(price)
    if parsed_price is not None and parsed_price > 0:
        # Keep original listing price separate from final negotiated price.
        embed["listing_price"] = parsed_price

    city_value = _clean_str(city)
    if city_value is not None:
        embed["city"] = city_value

    return embed


def _normalize_datetime_like_to_iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    text_value = _clean_str(value)
    if text_value is None:
        return None
    return text_value


def _build_booking_result_seller_embed(booking_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_booking_result_id = booking_doc.get("_id")
    if raw_booking_result_id is None:
        return None

    booking_result_id = str(raw_booking_result_id).strip()
    if not booking_result_id:
        return None

    embed: Dict[str, Any] = {"id": booking_result_id}

    booking_date = _normalize_datetime_like_to_iso(booking_doc.get("booking_date"))
    if booking_date is not None:
        embed["booking_date"] = booking_date

    raw_house_bought = booking_doc.get("house_bought")
    if isinstance(raw_house_bought, bool):
        embed["house_bought"] = raw_house_bought
    elif isinstance(raw_house_bought, (int, float)):
        embed["house_bought"] = bool(raw_house_bought)
    elif isinstance(raw_house_bought, str):
        house_bought_text = raw_house_bought.strip().lower()
        if house_bought_text in {"true", "1", "yes"}:
            embed["house_bought"] = True
        elif house_bought_text in {"false", "0", "no"}:
            embed["house_bought"] = False

    final_price = _parse_float(booking_doc.get("final_price"))
    if final_price is not None and final_price > 0:
        embed["final_price"] = final_price

    raw_house = booking_doc.get("house")
    if isinstance(raw_house, dict):
        house_embed: Dict[str, Any] = {}
        house_id = _clean_str(raw_house.get("id") or raw_house.get("_id"))
        if house_id is not None:
            house_embed["id"] = house_id

        listing_price = _parse_float(raw_house.get("listing_price"))
        if listing_price is None:
            listing_price = _parse_float(raw_house.get("price"))
        if listing_price is not None and listing_price > 0:
            house_embed["listing_price"] = listing_price

        house_city = _clean_str(raw_house.get("city"))
        if house_city is not None:
            house_embed["city"] = house_city

        house_zip_code = _clean_str(raw_house.get("zip_code"))
        if house_zip_code is not None:
            house_embed["zip_code"] = house_zip_code

        if house_embed:
            embed["house"] = house_embed

    return embed


def _build_feedback_seller_embed(
    raw_seller: Any,
    seller_context_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_seller, dict):
        return None

    raw_seller_id = raw_seller.get("id") or raw_seller.get("_id")
    if raw_seller_id is None:
        return None
    seller_id = str(raw_seller_id).strip()
    if not seller_id:
        return None

    context = seller_context_by_id.get(seller_id, {}) if seller_context_by_id else {}
    full_name = (
        _clean_str(raw_seller.get("full_name"))
        or _clean_str(raw_seller.get("name"))
        or _clean_str(context.get("full_name"))
        or _clean_str(context.get("name"))
    )
    email = _clean_str(raw_seller.get("email")) or _clean_str(context.get("email"))
    phone = _clean_str(raw_seller.get("phone")) or _clean_str(context.get("phone"))

    seller_embed: Dict[str, Any] = {
        "id": seller_id,
        "full_name": full_name,
        "email": email,
        "phone": phone,
    }
    return {key: value for key, value in seller_embed.items() if value is not None}


def _load_seller_context_from_db(users_collection: Any) -> Dict[str, Dict[str, Any]]:
    projection = {
        "_id": 1,
        "full_name": 1,
        "email": 1,
        "phone": 1,
    }
    seller_context_by_id: Dict[str, Dict[str, Any]] = {}
    for seller_doc in users_collection.find({"role": "seller"}, projection):
        seller_oid = seller_doc.get("_id")
        if seller_oid is None:
            continue
        seller_id = str(seller_oid).strip()
        if not seller_id:
            continue
        seller_context_by_id[seller_id] = {
            "id": seller_id,
            "full_name": _clean_str(seller_doc.get("full_name")),
            "email": _clean_str(seller_doc.get("email")),
            "phone": _clean_str(seller_doc.get("phone")),
        }
    return seller_context_by_id


def _deterministic_final_price(
    listing_price: float,
    house_id: str,
    ratio_getter: Optional[Callable[[str, str], float]] = None,
) -> float:
    safe_listing = max(float(listing_price), 1.0)
    ratio_fn = ratio_getter or _deterministic_ratio
    spread = max(PURCHASE_FINAL_PRICE_MAX_RATIO - PURCHASE_FINAL_PRICE_MIN_RATIO, 0.0)
    price_ratio = ratio_fn("booking_result_price", house_id)
    multiplier = PURCHASE_FINAL_PRICE_MIN_RATIO + (price_ratio * spread)
    if multiplier < PURCHASE_FINAL_PRICE_MIN_RATIO:
        multiplier = PURCHASE_FINAL_PRICE_MIN_RATIO
    if multiplier > PURCHASE_FINAL_PRICE_MAX_RATIO:
        multiplier = PURCHASE_FINAL_PRICE_MAX_RATIO
    final_price = min(safe_listing, safe_listing * multiplier)
    return round(max(final_price, 1.0), 2)


def _ensure_booking_result_outcome_mix(
    booking_results_docs: List[Dict[str, Any]],
    selected_houses: List[Dict[str, Any]],
    booking_results_max: int,
    ratio_getter: Optional[Callable[[str, str], float]] = None,
    fallback_candidates: Optional[Dict[bool, Optional[Dict[str, Any]]]] = None,
) -> None:
    safe_max = max(int(booking_results_max), 0)
    if safe_max <= 0 or not booking_results_docs:
        return

    has_bought_true = any(bool(doc.get("house_bought")) for doc in booking_results_docs)
    has_bought_false = any(not bool(doc.get("house_bought")) for doc in booking_results_docs)
    if has_bought_true and has_bought_false:
        return

    existing_ids = {str(doc.get("_id")) for doc in booking_results_docs}
    fallback_map: Dict[bool, Optional[Dict[str, Any]]] = {
        True: None,
        False: None,
    }
    if fallback_candidates:
        for outcome in (True, False):
            candidate = fallback_candidates.get(outcome)
            if not isinstance(candidate, dict):
                continue
            candidate_house_id = str(candidate.get("house_id") or "").strip()
            candidate_seller = candidate.get("seller")
            if (
                candidate_house_id
                and candidate_house_id not in existing_ids
                and isinstance(candidate_seller, dict)
            ):
                fallback_map[outcome] = candidate
    for house in selected_houses:
        house_id = str(house.get("house_id") or "").strip()
        if not house_id:
            continue
        seller_embed = house.get("seller")
        if not isinstance(seller_embed, dict):
            continue
        key = house_id
        if key in existing_ids:
            continue
        house_bought = bool(house.get("house_bought"))
        if fallback_map[house_bought] is None:
            fallback_map[house_bought] = house
        if fallback_map[True] is not None and fallback_map[False] is not None:
            break

    def _append_outcome(target_outcome: bool) -> None:
        house = fallback_map[target_outcome]
        if not house:
            return

        house_id = str(house["house_id"])
        price = _parse_float(house.get("price"))
        if price is None or price <= 0:
            return
        seller_embed = house.get("seller")
        if not isinstance(seller_embed, dict):
            return

        replace_index: Optional[int] = None
        if len(booking_results_docs) >= safe_max:
            for idx, existing_doc in enumerate(booking_results_docs):
                if bool(existing_doc.get("house_bought")) != target_outcome:
                    replace_index = idx
                    break
            if replace_index is None:
                return

        if target_outcome:
            booking_final_price = _deterministic_final_price(
                listing_price=price,
                house_id=house_id,
                ratio_getter=ratio_getter,
            )

        booking_doc: Dict[str, Any] = {
            "_id": house_id,
            "house_bought": target_outcome,
            "house": _build_booking_result_house_embed(
                house_id=house_id,
                price=price,
                city=house.get("city"),
            ),
            "seller": seller_embed,
        }
        if target_outcome:
            booking_doc["final_price"] = booking_final_price
        if replace_index is None:
            booking_results_docs.append(booking_doc)
        else:
            booking_results_docs[replace_index] = booking_doc

    if not has_bought_true:
        _append_outcome(True)
    if not has_bought_false:
        _append_outcome(False)


def _ensure_feedback_comment_mix(
    feedback_docs: List[Dict[str, Any]],
    feedbacks_by_buyer: Dict[ObjectId, List[Dict[str, Any]]],
) -> None:
    if len(feedback_docs) < 2:
        return

    has_comment = any(bool(str(doc.get("comment", "")).strip()) for doc in feedback_docs)
    has_no_comment = any("comment" not in doc or not str(doc.get("comment", "")).strip() for doc in feedback_docs)
    if has_comment and has_no_comment:
        return

    target_doc: Optional[Dict[str, Any]] = None
    for doc in reversed(feedback_docs):
        if bool(str(doc.get("comment", "")).strip()):
            target_doc = doc
            break
    if target_doc is None:
        return

    target_feedback_id = str(target_doc.get("id") or target_doc.get("_id") or "").strip()
    if not target_feedback_id:
        return
    target_doc.pop("comment", None)

    for buyer_feedbacks in feedbacks_by_buyer.values():
        for embed in buyer_feedbacks:
            embed_id = str(embed.get("id") or "").strip()
            if embed_id == target_feedback_id:
                embed.pop("comment", None)
                return


def _generate_synthetic_analytics_data(
    houses_collection: Any,
    users_collection: Any,
    booking_results_collection: Any,
    feedbacks_collection: Any,
    buyer_oids: List[ObjectId],
    buyer_embeds: Dict[ObjectId, Dict[str, Any]],
    booking_results_max: int,
    feedback_max: int,
    max_feedback_per_user: int,
    feedback_user_share: float,
) -> Dict[str, int]:
    booking_results_docs: List[Dict[str, Any]] = []
    selected_houses: List[Dict[str, Any]] = []
    fallback_houses_by_outcome: Dict[bool, Optional[Dict[str, Any]]] = {
        True: None,
        False: None,
    }

    safe_booking_results_max = max(int(booking_results_max), 0)
    safe_feedback_max = max(int(feedback_max), 0)
    safe_max_feedback_per_user = max(int(max_feedback_per_user), 0)
    safe_feedback_user_share = min(max(float(feedback_user_share), 0.0), 1.0)
    ratio_cache_local: Dict[str, float] = {}
    seller_context_by_id = _load_seller_context_from_db(users_collection)

    def _ratio(prefix: str, marker: str) -> float:
        key = f"{prefix}|{marker}"
        cached = ratio_cache_local.get(key)
        if cached is not None:
            return cached
        value = _deterministic_ratio(prefix, marker)
        ratio_cache_local[key] = value
        return value

    house_projection = {
        "_id": 1,
        "is_sold": 1,
        "price": 1,
        "city": 1,
        "for_sale_by": 1,
    }
    base_booking_dt = datetime(2023, 1, 1, tzinfo=timezone.utc)
    booking_months_span = 24
    sold_house_ids: set[str] = set()

    def _mark_houses_sold(house_ids: List[str], batch_size: int = 2000) -> None:
        if not house_ids:
            return
        safe_batch = max(int(batch_size), 1)
        for start in range(0, len(house_ids), safe_batch):
            chunk = house_ids[start : start + safe_batch]
            oids: List[ObjectId] = []
            for raw_id in chunk:
                try:
                    oids.append(ObjectId(str(raw_id)))
                except Exception:
                    continue
            if not oids:
                continue
            _run_with_retry(
                lambda oids=oids: houses_collection.update_many(
                    {"_id": {"$in": oids}, "is_sold": {"$ne": True}},
                    {"$set": {"is_sold": True}},
                ),
                f"houses.update_many_mark_sold[{start}:{start + len(chunk)}]",
            )

    def _collect_booking_results_from_cursor(
        cursor: Any,
    ) -> Tuple[int, Optional[ObjectId]]:
        scanned = 0
        last_seen_house_oid: Optional[ObjectId] = None
        parse_float = _parse_float
        append_booking = booking_results_docs.append
        append_selected = selected_houses.append

        for house_doc in cursor:
            scanned += 1

            house_oid = house_doc.get("_id")
            if not isinstance(house_oid, ObjectId):
                continue
            last_seen_house_oid = house_oid

            raw_price = house_doc.get("price")
            if isinstance(raw_price, (int, float)):
                price = float(raw_price)
            else:
                price = parse_float(raw_price)
            if price is None or price <= 0:
                continue

            house_id = str(house_oid)
            seller_embed = _build_feedback_seller_embed(
                house_doc.get("for_sale_by"),
                seller_context_by_id=seller_context_by_id,
            )
            if not isinstance(seller_embed, dict):
                continue
            # Booking outcome is synthetic and independent from current house listing status.
            house_bought = _ratio("booking_result_outcome", house_id) < DEFAULT_BOOKING_PURCHASE_RATE
            if fallback_houses_by_outcome[house_bought] is None:
                fallback_houses_by_outcome[house_bought] = {
                    "house_id": house_id,
                    "house_bought": house_bought,
                    "price": price,
                    "city": house_doc.get("city"),
                    "seller": seller_embed,
                }
            if len(booking_results_docs) >= safe_booking_results_max:
                if (
                    fallback_houses_by_outcome[True] is not None
                    and fallback_houses_by_outcome[False] is not None
                ):
                    break
                continue
            threshold = (
                DEFAULT_SOLD_BOOKING_SELECTION_RATE
                if house_bought
                else DEFAULT_ACTIVE_BOOKING_SELECTION_RATE
            )
            if _ratio("booking_result_select", house_id) >= threshold:
                continue

            month_ratio = _ratio("booking_result_month", house_id)
            day_ratio = _ratio("booking_result_day", house_id)
            month_index = min(int(month_ratio * booking_months_span), booking_months_span - 1)
            day_offset = min(int(day_ratio * 29), 28)
            booking_date = base_booking_dt + timedelta(days=(month_index * 30) + day_offset)

            booking_doc: Dict[str, Any] = {
                "_id": house_id,
                "house_bought": house_bought,
                "booking_date": booking_date,
                "house": _build_booking_result_house_embed(
                    house_id=house_id,
                    price=price,
                    city=house_doc.get("city"),
                ),
                "seller": seller_embed,
            }
            if house_bought:
                booking_doc["final_price"] = _deterministic_final_price(
                    listing_price=price,
                    house_id=house_id,
                    ratio_getter=_ratio,
                )
                sold_house_ids.add(house_id)
            append_booking(booking_doc)
            append_selected(
                {
                    "house_id": house_id,
                    "house_bought": house_bought,
                    "price": price,
                    "city": house_doc.get("city"),
                    "seller": seller_embed,
                }
            )
        return scanned, last_seen_house_oid

    if safe_booking_results_max > 0:
        conservative_threshold = min(
            DEFAULT_SOLD_BOOKING_SELECTION_RATE,
            DEFAULT_ACTIVE_BOOKING_SELECTION_RATE,
        )
        initial_scan_limit = max(
            BOOKING_SCAN_MIN_ROWS,
            safe_booking_results_max,
            int(
                (safe_booking_results_max / max(conservative_threshold, 1e-9))
                * BOOKING_SCAN_SAFETY_MULTIPLIER
            ),
        )
        first_cursor = (
            houses_collection.find({}, house_projection)
            .sort("_id", 1)
            .limit(initial_scan_limit)
        )
        scanned_count, last_house_oid = _collect_booking_results_from_cursor(first_cursor)

        if (
            len(booking_results_docs) < safe_booking_results_max
            and scanned_count >= initial_scan_limit
            and last_house_oid is not None
        ):
            remaining_cursor = houses_collection.find(
                {"_id": {"$gt": last_house_oid}},
                house_projection,
            ).sort("_id", 1)
            _collect_booking_results_from_cursor(remaining_cursor)

    _ensure_booking_result_outcome_mix(
        booking_results_docs=booking_results_docs,
        selected_houses=selected_houses,
        booking_results_max=safe_booking_results_max,
        ratio_getter=_ratio,
        fallback_candidates=fallback_houses_by_outcome,
    )

    # Enforce coherence between booking outcomes and house inventory: any purchase marks the house as sold.
    sold_house_ids_from_mix = [
        str(doc.get("_id"))
        for doc in booking_results_docs
        if bool(doc.get("house_bought")) and doc.get("_id") is not None
    ]
    for house_id in sold_house_ids_from_mix:
        sold_house_ids.add(house_id)
    _mark_houses_sold(sorted(sold_house_ids))

    _insert_many_in_batches(
        booking_results_collection,
        booking_results_docs,
        batch_size=DEFAULT_BOOKING_INSERT_BATCH_SIZE,
        op_name="booking_results",
    )

    seller_booking_results_by_oid: Dict[ObjectId, List[Dict[str, Any]]] = defaultdict(list)
    for booking_doc in booking_results_docs:
        raw_seller = booking_doc.get("seller")
        if not isinstance(raw_seller, dict):
            continue
        seller_oid = _coerce_object_id(raw_seller.get("id"))
        if seller_oid is None:
            continue
        seller_booking_embed = _build_booking_result_seller_embed(booking_doc)
        if seller_booking_embed is None:
            continue
        seller_booking_results_by_oid[seller_oid].append(seller_booking_embed)

    feedback_docs: List[Dict[str, Any]] = []
    feedbacks_by_buyer: Dict[ObjectId, List[Dict[str, Any]]] = defaultdict(list)
    base_feedback_dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
    next_feedback_dt_by_buyer: Dict[ObjectId, datetime] = {}

    eligible_buyer_oids: List[ObjectId] = []
    for buyer_oid in buyer_oids:
        if _ratio("feedback_user_enabled", str(buyer_oid)) < safe_feedback_user_share:
            eligible_buyer_oids.append(buyer_oid)
    if not eligible_buyer_oids and buyer_oids and safe_feedback_user_share > 0:
        eligible_buyer_oids = [buyer_oids[0]]

    if eligible_buyer_oids and safe_feedback_max and safe_max_feedback_per_user > 0:
        for selected in selected_houses:
            if len(feedback_docs) >= safe_feedback_max:
                break

            house_id = selected["house_id"]
            if _ratio("feedback_select", house_id) >= DEFAULT_FEEDBACK_SELECTION_RATE:
                continue

            buyer_ratio = _ratio("feedback_buyer", house_id)
            buyer_index = min(int(buyer_ratio * len(eligible_buyer_oids)), len(eligible_buyer_oids) - 1)
            buyer_oid = eligible_buyer_oids[buyer_index]
            if len(feedbacks_by_buyer[buyer_oid]) >= safe_max_feedback_per_user:
                continue
            buyer_embed = buyer_embeds.get(buyer_oid, {"id": buyer_oid})
            seller_embed = _build_feedback_seller_embed(
                selected.get("seller"),
                seller_context_by_id=seller_context_by_id,
            )

            rating = _deterministic_feedback_rating(
                bool(selected["house_bought"]),
                house_id,
                ratio_getter=_ratio,
            )
            comment = _deterministic_feedback_comment(
                rating,
                house_id,
                ratio_getter=_ratio,
            )
            candidate_feedback_datetime = base_feedback_dt + timedelta(minutes=len(feedback_docs))
            next_allowed_feedback_datetime = next_feedback_dt_by_buyer.get(
                buyer_oid,
                base_feedback_dt,
            )
            feedback_datetime = max(candidate_feedback_datetime, next_allowed_feedback_datetime)
            gap_ratio = _ratio(
                "feedback_gap_jitter",
                f"{buyer_oid}:{house_id}:{len(feedbacks_by_buyer[buyer_oid])}",
            )
            gap_jitter_hours = int(gap_ratio * (DEFAULT_FEEDBACK_GAP_JITTER_MAX_HOURS + 1))
            next_feedback_dt_by_buyer[buyer_oid] = feedback_datetime + timedelta(
                hours=DEFAULT_FEEDBACK_MIN_GAP_HOURS + gap_jitter_hours
            )
            feedback_date = feedback_datetime.isoformat().replace("+00:00", "Z")
            call_datetime = (feedback_datetime - timedelta(hours=4)).isoformat().replace("+00:00", "Z")
            feedback_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"houseup-callslot:{buyer_oid}:{house_id}",
            ).hex

            feedback_doc: Dict[str, Any] = {
                "_id": feedback_id,
                "feedback_date": feedback_date,
                "call_datetime": call_datetime,
                "rating": rating,
                "user": {
                    "id": str(buyer_oid),
                    "full_name": buyer_embed.get("full_name"),
                    "email": buyer_embed.get("email"),
                    "phone": buyer_embed.get("phone"),
                },
            }
            if comment is not None:
                feedback_doc["comment"] = comment
            if seller_embed is not None:
                feedback_doc["seller"] = seller_embed

            feedback_doc["user"] = {
                key: value for key, value in feedback_doc["user"].items() if value is not None
            }

            feedback_docs.append(feedback_doc)
            buyer_feedback_embed = {
                "id": feedback_id,
                "feedback_date": feedback_date,
                "call_datetime": call_datetime,
                "rating": rating,
                "comment": comment,
                "seller": seller_embed,
            }
            feedbacks_by_buyer[buyer_oid].append(
                {k: v for k, v in buyer_feedback_embed.items() if v is not None}
            )

        if not feedback_docs and selected_houses and eligible_buyer_oids:
            selected = selected_houses[0]
            house_id = selected["house_id"]
            buyer_oid = eligible_buyer_oids[0]
            buyer_embed = buyer_embeds.get(buyer_oid, {"id": buyer_oid})
            seller_embed = _build_feedback_seller_embed(
                selected.get("seller"),
                seller_context_by_id=seller_context_by_id,
            )
            rating = _deterministic_feedback_rating(
                bool(selected["house_bought"]),
                house_id,
                ratio_getter=_ratio,
            )
            comment = _deterministic_feedback_comment(
                rating,
                house_id,
                ratio_getter=_ratio,
            )
            feedback_date = base_feedback_dt.isoformat().replace("+00:00", "Z")
            call_datetime = (base_feedback_dt - timedelta(hours=4)).isoformat().replace("+00:00", "Z")
            feedback_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"houseup-callslot:{buyer_oid}:{house_id}",
            ).hex
            feedback_doc: Dict[str, Any] = {
                "_id": feedback_id,
                "feedback_date": feedback_date,
                "call_datetime": call_datetime,
                "rating": rating,
                "user": {
                    "id": str(buyer_oid),
                    "full_name": buyer_embed.get("full_name"),
                    "email": buyer_embed.get("email"),
                    "phone": buyer_embed.get("phone"),
                },
            }
            if comment is not None:
                feedback_doc["comment"] = comment
            if seller_embed is not None:
                feedback_doc["seller"] = seller_embed
            feedback_doc["user"] = {
                key: value for key, value in feedback_doc["user"].items() if value is not None
            }
            feedback_docs.append(feedback_doc)
            buyer_feedback_embed = {
                "id": feedback_id,
                "feedback_date": feedback_date,
                "call_datetime": call_datetime,
                "rating": rating,
                "comment": comment,
                "seller": seller_embed,
            }
            feedbacks_by_buyer[buyer_oid].append(
                {k: v for k, v in buyer_feedback_embed.items() if v is not None}
            )

    _ensure_feedback_comment_mix(feedback_docs, feedbacks_by_buyer)

    _insert_many_in_batches(
        feedbacks_collection,
        feedback_docs,
        batch_size=DEFAULT_FEEDBACK_INSERT_BATCH_SIZE,
        op_name="feedbacks",
    )

    user_ops: List[UpdateOne] = []
    seller_oids_for_sync: List[ObjectId] = []
    for seller_id in seller_context_by_id.keys():
        seller_oid = _coerce_object_id(seller_id)
        if seller_oid is not None:
            seller_oids_for_sync.append(seller_oid)
    for seller_oid in seller_oids_for_sync:
        raw_booking_results = seller_booking_results_by_oid.get(seller_oid, [])
        sorted_booking_results = sorted(
            raw_booking_results,
            key=lambda item: (item.get("booking_date", ""), item.get("id", "")),
            reverse=True,
        )

        set_payload: Dict[str, Any] = {}
        unset_payload: Dict[str, str] = {}
        if sorted_booking_results:
            set_payload["booking_results"] = sorted_booking_results
        else:
            unset_payload["booking_results"] = ""

        update_payload: Dict[str, Any] = {}
        if set_payload:
            update_payload["$set"] = set_payload
        if unset_payload:
            update_payload["$unset"] = unset_payload
        if update_payload:
            user_ops.append(
                UpdateOne(
                    {"_id": seller_oid},
                    update_payload,
                    upsert=False,
                )
            )

    for buyer_oid in buyer_oids:
        raw_feedbacks = feedbacks_by_buyer.get(buyer_oid, [])
        sorted_feedbacks = sorted(
            raw_feedbacks,
            key=lambda item: (item.get("feedback_date", ""), item.get("id", "")),
            reverse=True,
        )

        set_payload: Dict[str, Any] = {}
        unset_payload: Dict[str, str] = {}
        if sorted_feedbacks:
            set_payload["feedbacks"] = sorted_feedbacks
        else:
            unset_payload["feedbacks"] = ""

        update_payload: Dict[str, Any] = {}
        if set_payload:
            update_payload["$set"] = set_payload
        if unset_payload:
            update_payload["$unset"] = unset_payload
        if update_payload:
            user_ops.append(
                UpdateOne(
                    {"_id": buyer_oid},
                    update_payload,
                    upsert=False,
                )
            )

    if user_ops:
        _run_with_retry(
            lambda: users_collection.bulk_write(user_ops, ordered=False),
            "users.bulk_write_feedbacks",
        )

    feedback_users = sum(1 for feedbacks in feedbacks_by_buyer.values() if feedbacks)
    return {
        "booking_results": len(booking_results_docs),
        "feedbacks": len(feedback_docs),
        "buyers_with_feedback": feedback_users,
    }


def _reset_collection(collection: Any, reset_mode: str, label: str) -> None:
    if reset_mode == RESET_NONE:
        return
    if reset_mode == RESET_DROP:
        _run_with_retry(lambda: collection.drop(), f"{label}.drop")
        return
    _run_with_retry(lambda: collection.delete_many({}), f"{label}.delete_many")


def _load_buyer_context_from_db(
    users_collection: Any,
    expected_count: int = DEFAULT_SEED_BUYER_COUNT,
) -> Tuple[List[ObjectId], Dict[ObjectId, Dict[str, Any]]]:
    buyer_docs = list(
        users_collection.find(
            {
                "role": "buyer",
                "email": {"$regex": r"^buyer_\d+@houseup\.local$"},
            },
            {"_id": 1, "full_name": 1, "email": 1, "phone": 1},
        ).sort("email", 1)
    )

    if len(buyer_docs) < expected_count:
        buyer_docs = list(
            users_collection.find(
                {"role": "buyer"},
                {"_id": 1, "full_name": 1, "email": 1, "phone": 1},
            )
            .sort("email", 1)
            .limit(expected_count)
        )

    if not buyer_docs:
        raise ValueError("No buyers found in users collection. Run houses-users phase first.")

    buyer_oids: List[ObjectId] = []
    buyer_embeds: Dict[ObjectId, Dict[str, Any]] = {}
    for doc in buyer_docs:
        oid = doc.get("_id")
        if not isinstance(oid, ObjectId):
            continue
        buyer_oids.append(oid)
        buyer_embeds[oid] = {
            "id": oid,
            "full_name": doc.get("full_name"),
            "email": doc.get("email"),
            "phone": doc.get("phone"),
        }

    if not buyer_oids:
        raise ValueError("No valid buyer ObjectIds found in users collection.")
    return buyer_oids, buyer_embeds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load realtor dataset into MongoDB with deterministic sellers/buyers."
    )
    parser.add_argument(
        "--phase",
        choices=[PHASE_ALL, PHASE_HOUSES_USERS, PHASE_ANALYTICS],
        default=PHASE_ALL,
        help=(
            "Execution phase: "
            "'all' (full rebuild), "
            "'houses-users' (seed users + houses + user house lists), "
            "'analytics' (booking_results + feedbacks only)."
        ),
    )
    parser.add_argument(
        "--reset-mode",
        choices=[RESET_DELETE, RESET_DROP, RESET_NONE],
        default=RESET_DELETE,
        help=(
            "Reset strategy for collections touched by selected phase. "
            "'delete' keeps collection metadata/indexes, "
            "'drop' is usually faster on large datasets, "
            "'none' skips reset."
        ),
    )
    parser.add_argument(
        "--csv-path",
        default=str(DEFAULT_CSV_PATH),
        help="Path to the realtor CSV file.",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI"),
        help="MongoDB URI. Falls back to MONGO_URI.",
    )
    parser.add_argument(
        "--mongo-db",
        default=os.getenv("MONGO_DB", DEFAULT_MONGO_DB),
        help="MongoDB database name. Falls back to MONGO_DB.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="House batch size for bulk inserts.",
    )
    parser.add_argument(
        "--seed-password",
        default=DEFAULT_SEED_PASSWORD,
        help="Password used for seed users (stored as password_hash).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit for debugging. 0 means full dataset.",
    )
    parser.add_argument(
        "--seller-count",
        type=int,
        default=0,
        help=(
            "Override seed seller count. "
            "0 enables automatic sizing based on total rows and target houses per seller."
        ),
    )
    parser.add_argument(
        "--buyer-count",
        type=int,
        default=0,
        help=(
            "Override seed buyer count. "
            "0 enables automatic sizing based on sold rows and max bought houses per buyer."
        ),
    )
    parser.add_argument(
        "--write-threads",
        type=int,
        default=DEFAULT_WRITE_THREADS,
        help="Parallel threads for house write batches. 1 disables threading.",
    )
    parser.add_argument(
        "--max-inflight-batches",
        type=int,
        default=DEFAULT_MAX_INFLIGHT_BATCHES,
        help="Max in-flight house batches when write-threads > 1.",
    )
    parser.add_argument(
        "--user-flush-items",
        type=int,
        default=DEFAULT_USER_FLUSH_ITEMS,
        help="Flush pending user list updates every N accepted embeds.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Print progress every N processed rows.",
    )
    parser.add_argument(
        "--booking-results-max",
        type=int,
        default=DEFAULT_BOOKING_RESULTS_MAX,
        help="Maximum synthetic booking_results documents to generate.",
    )
    parser.add_argument(
        "--feedback-max",
        type=int,
        default=DEFAULT_FEEDBACK_MAX,
        help="Maximum synthetic feedback documents to generate.",
    )
    parser.add_argument(
        "--max-feedback-per-user",
        type=int,
        default=DEFAULT_MAX_FEEDBACK_PER_USER,
        help="Maximum total feedback documents generated for each buyer.",
    )
    parser.add_argument(
        "--feedback-user-share",
        type=float,
        default=DEFAULT_FEEDBACK_USER_SHARE,
        help="Share of buyers eligible to receive synthetic feedback (0..1).",
    )
    parser.add_argument(
        "--bootstrap-user-email-index",
        action="store_true",
        help=(
            "Create users.email unique index during bootstrap. "
            "Disabled by default for faster/degraded-cluster imports."
        ),
    )
    parser.add_argument(
        "--skip-synthetic-analytics-data",
        action="store_true",
        help="Skip synthetic generation for booking_results and feedbacks.",
    )
    parser.add_argument(
        "--disable-fast-mode",
        action="store_true",
        help=(
            "Disable FAST post-pass mode for user house lists. "
            "By default FAST mode is enabled and user lists are rebuilt after houses ingest."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.mongo_uri:
        raise ValueError("Mongo URI missing. Set --mongo-uri or MONGO_URI.")
    if not args.mongo_db:
        raise ValueError("Mongo DB missing. Set --mongo-db or MONGO_DB.")
    if str(args.mongo_db).strip().lower() != DEFAULT_MONGO_DB:
        raise ValueError(
            f"Invalid Mongo DB '{args.mongo_db}'. This loader must run on '{DEFAULT_MONGO_DB}'."
        )

    phase = str(args.phase).strip().lower()
    reset_mode = str(args.reset_mode).strip().lower()

    csv_path = Path(args.csv_path)
    if phase in {PHASE_ALL, PHASE_HOUSES_USERS} and not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    batch_size = max(int(args.batch_size), 1)
    row_limit = max(int(args.limit), 0)
    seller_count_override = max(int(args.seller_count), 0)
    buyer_count_override = max(int(args.buyer_count), 0)
    write_threads = max(int(args.write_threads), 1)
    max_inflight_batches = max(int(args.max_inflight_batches), 1)
    user_flush_items = max(int(args.user_flush_items), 1)
    progress_every = max(int(args.progress_every), 1)
    booking_results_max = max(int(args.booking_results_max), 0)
    feedback_max = max(int(args.feedback_max), 0)
    max_feedback_per_user = max(int(args.max_feedback_per_user), 0)
    feedback_user_share = min(max(float(args.feedback_user_share), 0.0), 1.0)
    fast_mode = not bool(args.disable_fast_mode)

    mongo_client = MongoClient(args.mongo_uri)
    _ensure_writable_primary(mongo_client)
    mongo_db = mongo_client[args.mongo_db]
    houses_collection = mongo_db["houses"]
    users_collection = mongo_db["users"]
    booking_results_collection = mongo_db["booking_results"]
    feedbacks_collection = mongo_db["feedbacks"]

    total_rows = 0
    sold_rows = 0
    active_rows = 0
    buyer_cursor = 0
    buyer_oids: List[ObjectId] = []
    buyer_embeds: Dict[ObjectId, Dict[str, Any]] = {}
    seller_count = seller_count_override or DEFAULT_SEED_SELLER_COUNT
    buyer_count = buyer_count_override or DEFAULT_SEED_BUYER_COUNT

    if phase in {PHASE_ALL, PHASE_HOUSES_USERS}:
        pre_total_rows, pre_sold_rows, pre_active_rows = _compute_dataset_counts(csv_path, row_limit=row_limit)
        seller_count, buyer_count = _derive_seed_user_counts(
            total_rows=pre_total_rows,
            sold_rows=pre_sold_rows,
            seller_count_override=seller_count_override,
            buyer_count_override=buyer_count_override,
        )
        print(
            f"CSV pre-scan rows={pre_total_rows} sold={pre_sold_rows} active={pre_active_rows} "
            f"(limit={row_limit or 'full'})"
        )
        print(
            f"Seed sizing seller_count={seller_count} buyer_count={buyer_count} "
            f"(seller_target_houses={DEFAULT_SELLER_TARGET_HOUSES}, "
            f"buyer_max_bought={DEFAULT_BUYER_MAX_BOUGHT_HOUSES})"
        )

        (
            seed_users,
            seller_embeds,
            seller_oids,
            buyer_oids,
            buyer_embeds,
            seed_emails,
        ) = _build_seed_users(
            args.seed_password,
            seller_count=seller_count,
            buyer_count=buyer_count,
        )
        buyer_cursor = 0

        print(f"Rebuild start ({phase}): reset_mode={reset_mode}...")
        if phase == PHASE_ALL:
            _reset_collection(houses_collection, reset_mode, "houses")
            _reset_collection(users_collection, reset_mode, "users")
            _reset_collection(booking_results_collection, reset_mode, "booking_results")
            _reset_collection(feedbacks_collection, reset_mode, "feedbacks")
        else:
            _reset_collection(houses_collection, reset_mode, "houses")
            _reset_collection(users_collection, reset_mode, "users")

        if reset_mode == RESET_NONE:
            _run_with_retry(
                lambda: users_collection.delete_many({"email": {"$in": seed_emails}}),
                "users.delete_many_seed_emails",
            )

        _run_with_retry(
            lambda: users_collection.insert_many(seed_users, ordered=False),
            "users.insert_many",
        )
        if args.bootstrap_user_email_index:
            _run_with_retry(
                lambda: users_collection.create_index(
                    "email",
                    unique=True,
                    name="idx_users_email_unique",
                ),
                "users.create_index",
            )

        house_docs_pending: List[Dict[str, Any]] = []
        house_futures: List[Future] = []
        house_executor: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(max_workers=write_threads, thread_name_prefix="house-writer")
            if write_threads > 1
            else None
        )

        seller_for_sale_states: Dict[ObjectId, _HouseListState] = {}
        seller_sold_states: Dict[ObjectId, _HouseListState] = {}
        buyer_bought_states: Dict[ObjectId, _HouseListState] = {}
        pending_user_items = 0

        parse_float = _parse_float
        parse_int = _parse_int
        clean_str = _clean_str
        promote_latest_house_embed = _promote_latest_house_embed
        flush_houses = _flush_houses
        flush_user_updates = _flush_user_updates
        seller_count_safe = len(seller_oids)
        buyer_count_safe = len(buyer_oids)

        print(f"Importing dataset from: {csv_path}")
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
                reader = csv.DictReader(csv_file)
                for row_number, row in enumerate(reader, start=1):
                    if row_limit and row_number > row_limit:
                        break

                    total_rows += 1
                    status_raw = str(row.get("status", "")).strip().lower()
                    is_sold = status_raw == "sold"
                    if is_sold:
                        sold_rows += 1
                    else:
                        active_rows += 1

                    row_get = row.get
                    house_oid = ObjectId()
                    price = parse_float(row_get("price"))
                    city = clean_str(row_get("city"))
                    if seller_count_safe <= 0:
                        raise RuntimeError("No seller users available for assignment.")
                    seller_index = (total_rows - 1) % seller_count_safe
                    seller_oid = seller_oids[seller_index]

                    house_doc = {
                        "_id": house_oid,
                        "is_sold": is_sold,
                        "for_sale_by": seller_embeds[seller_index],
                    }
                    if price is not None:
                        house_doc["price"] = price
                    bed = parse_int(row_get("bed"))
                    if bed is not None:
                        house_doc["bed"] = bed
                    bath = parse_int(row_get("bath"))
                    if bath is not None:
                        house_doc["bath"] = bath
                    if city is not None:
                        house_doc["city"] = city
                    state = clean_str(row_get("state"))
                    if state is not None:
                        house_doc["state"] = state
                    zip_code = clean_str(row_get("zip_code"))
                    if zip_code is not None:
                        house_doc["zip_code"] = zip_code
                    house_size = parse_float(row_get("house_size"))
                    if house_size is not None:
                        house_doc["house_size"] = house_size
                    prev_sold_date = clean_str(row_get("prev_sold_date"))
                    if prev_sold_date is not None:
                        house_doc["prev_sold_date"] = prev_sold_date
                    house_docs_pending.append(house_doc)

                    house_embed: Dict[str, Any] = {"id": str(house_oid)}
                    if city is not None:
                        house_embed["city"] = city
                    if zip_code is not None:
                        house_embed["zip_code"] = zip_code

                    if fast_mode:
                        if is_sold and buyer_count_safe > 0:
                            buyer_cursor += 1
                    else:
                        seller_state_target = seller_sold_states if is_sold else seller_for_sale_states
                        promote_latest_house_embed(
                            seller_state_target,
                            seller_oid,
                            house_embed,
                        )
                        pending_user_items += 1
                        if is_sold and buyer_count_safe > 0:
                            buyer_oid = buyer_oids[buyer_cursor % buyer_count_safe]
                            promote_latest_house_embed(
                                buyer_bought_states,
                                buyer_oid,
                                house_embed,
                            )
                            pending_user_items += 1
                            buyer_cursor += 1

                    if len(house_docs_pending) >= batch_size:
                        flush_houses(
                            houses_collection=houses_collection,
                            house_docs_pending=house_docs_pending,
                            house_executor=house_executor,
                            house_futures=house_futures,
                            max_inflight_batches=max_inflight_batches,
                        )

                    if not fast_mode and pending_user_items >= user_flush_items:
                        flushed = flush_user_updates(
                            users_collection=users_collection,
                            seller_for_sale_states=seller_for_sale_states,
                            seller_sold_states=seller_sold_states,
                            buyer_bought_states=buyer_bought_states,
                        )
                        pending_user_items = max(pending_user_items - flushed, 0)

                    if total_rows % progress_every == 0:
                        print(
                            f"Progress rows={total_rows} sold={sold_rows} active={active_rows} "
                            f"buyer_assigned={buyer_cursor} inflight_house_batches={len(house_futures)}"
                        )

            _flush_houses(
                houses_collection=houses_collection,
                house_docs_pending=house_docs_pending,
                house_executor=house_executor,
                house_futures=house_futures,
                max_inflight_batches=max_inflight_batches,
            )

            _drain_house_futures(house_futures)

            if fast_mode:
                # FAST mode rebuilds user lists after all house mutations (analytics may mark houses sold).
                if phase == PHASE_HOUSES_USERS:
                    post_pass_buyer_cursor = _rebuild_user_house_lists_post_pass(
                        houses_collection=houses_collection,
                        users_collection=users_collection,
                        buyer_oids=buyer_oids,
                    )
                    buyer_cursor = post_pass_buyer_cursor
            else:
                _flush_user_updates(
                    users_collection=users_collection,
                    seller_for_sale_states=seller_for_sale_states,
                    seller_sold_states=seller_sold_states,
                    buyer_bought_states=buyer_bought_states,
                )
        finally:
            if house_executor is not None:
                house_executor.shutdown(wait=True)
    else:
        buyer_oids, buyer_embeds = _load_buyer_context_from_db(
            users_collection,
            expected_count=buyer_count,
        )

    synthetic_stats = {"booking_results": 0, "feedbacks": 0, "buyers_with_feedback": 0}
    generate_feedbacks = (
        feedback_max > 0
        and max_feedback_per_user > 0
        and feedback_user_share > 0.0
    )
    if phase in {PHASE_ALL, PHASE_ANALYTICS}:
        if phase == PHASE_ANALYTICS:
            _reset_collection(booking_results_collection, reset_mode, "booking_results")
            if generate_feedbacks:
                _reset_collection(feedbacks_collection, reset_mode, "feedbacks")

        if args.skip_synthetic_analytics_data:
            print("Synthetic analytics generation skipped (--skip-synthetic-analytics-data).")
        else:
            print("Generating synthetic booking_results and feedbacks...")
            if not buyer_oids:
                buyer_oids, buyer_embeds = _load_buyer_context_from_db(users_collection)
            synthetic_stats = _generate_synthetic_analytics_data(
                houses_collection=houses_collection,
                users_collection=users_collection,
                booking_results_collection=booking_results_collection,
                feedbacks_collection=feedbacks_collection,
                buyer_oids=buyer_oids,
                buyer_embeds=buyer_embeds,
                booking_results_max=booking_results_max,
                feedback_max=feedback_max,
                max_feedback_per_user=max_feedback_per_user,
                feedback_user_share=feedback_user_share,
            )
    else:
        print("Analytics phase skipped (phase=houses-users).")

    if phase in {PHASE_ALL, PHASE_ANALYTICS}:
        post_pass_buyer_cursor = _rebuild_user_house_lists_post_pass(
            houses_collection=houses_collection,
            users_collection=users_collection,
            buyer_oids=buyer_oids,
        )
        buyer_cursor = post_pass_buyer_cursor

    final_houses = _run_with_retry(
        lambda: houses_collection.count_documents({}),
        "houses.count_documents",
    )
    final_users = _run_with_retry(
        lambda: users_collection.count_documents({}),
        "users.count_documents",
    )
    final_booking_results = _run_with_retry(
        lambda: booking_results_collection.count_documents({}),
        "booking_results.count_documents",
    )
    final_feedbacks = _run_with_retry(
        lambda: feedbacks_collection.count_documents({}),
        "feedbacks.count_documents",
    )

    print(f"Import completed (phase={phase}, reset_mode={reset_mode}).")
    print(f"Rows processed: {total_rows}")
    print(f"Houses in collection: {final_houses}")
    print(f"Users in collection: {final_users}")
    print(f"Sold rows: {sold_rows}")
    print(f"Active rows: {active_rows}")
    print(f"Buyer-owned sold houses assigned: {buyer_cursor}")
    print(f"Booking results generated: {synthetic_stats['booking_results']}")
    print(f"Feedback generated: {synthetic_stats['feedbacks']}")
    print(f"Buyers with feedback: {synthetic_stats['buyers_with_feedback']}")
    print(f"Booking results in collection: {final_booking_results}")
    print(f"Feedback in collection: {final_feedbacks}")


if __name__ == "__main__":
    main()
