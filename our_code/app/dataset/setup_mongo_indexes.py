#!/usr/bin/env python3
import argparse
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


DEFAULT_MONGO_DB = "houseup"


@dataclass(frozen=True)
class IndexSpec:
    keys: Sequence[Tuple[str, int]]
    name: str
    unique: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create MongoDB indexes for HouseUp collections."
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
        "--drop-extra",
        action="store_true",
        help="Drop indexes not defined by this script (keeps _id_).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without applying changes.",
    )
    return parser.parse_args()


def _specs() -> Dict[str, List[IndexSpec]]:
    return {
        "users": [
            IndexSpec([("email", ASCENDING)], name="idx_users_email_unique", unique=True),
            IndexSpec([("role", ASCENDING)], name="idx_users_role"),
            IndexSpec([("for_sale_houses.id", ASCENDING)], name="idx_users_for_sale_house_id"),
            IndexSpec([("sold_houses.id", ASCENDING)], name="idx_users_sold_house_id"),
            IndexSpec([("bought_houses.id", ASCENDING)], name="idx_users_bought_house_id"),
        ],
        "houses": [
            IndexSpec([("is_sold", ASCENDING)], name="idx_houses_is_sold"),
            IndexSpec([("city", ASCENDING)], name="idx_houses_city"),
            IndexSpec([("state", ASCENDING)], name="idx_houses_state"),
            IndexSpec([("zip_code", ASCENDING)], name="idx_houses_zip_code"),
            IndexSpec([("price", ASCENDING)], name="idx_houses_price"),
            IndexSpec([("bed", ASCENDING)], name="idx_houses_bed"),
            IndexSpec([("bath", ASCENDING)], name="idx_houses_bath"),
            IndexSpec([("house_size", ASCENDING)], name="idx_houses_house_size"),
            IndexSpec([("prev_sold_date", ASCENDING)], name="idx_houses_prev_sold_date"),
            IndexSpec([("for_sale_by.id", ASCENDING)], name="idx_houses_for_sale_by_id"),
            IndexSpec(
                [
                    ("is_sold", ASCENDING),
                    ("city", ASCENDING),
                    ("state", ASCENDING),
                    ("zip_code", ASCENDING),
                    ("_id", ASCENDING),
                ],
                name="idx_houses_search_location",
            ),
            IndexSpec(
                [
                    ("is_sold", ASCENDING),
                    ("price", ASCENDING),
                    ("_id", ASCENDING),
                ],
                name="idx_houses_search_price",
            ),
        ],
        "feedbacks": [
            IndexSpec([("rating", ASCENDING)], name="idx_feedbacks_rating"),
            IndexSpec([("feedback_date", ASCENDING)], name="idx_feedbacks_feedback_date"),
        ],
        "booking_results": [
            IndexSpec([("house_bought", ASCENDING)], name="idx_booking_results_house_bought"),
            IndexSpec([("booking_date", ASCENDING)], name="idx_booking_results_booking_date"),
        ],
    }


def _ensure_indexes(
    collection: Collection,
    specs: Iterable[IndexSpec],
    drop_extra: bool,
    dry_run: bool,
) -> None:
    existing = collection.index_information()
    target_names = {spec.name for spec in specs}
    target_names.add("_id_")

    if drop_extra:
        for name in sorted(existing.keys()):
            if name in target_names:
                continue
            if dry_run:
                print(f"[dry-run] drop {collection.name}.{name}")
                continue
            collection.drop_index(name)
            print(f"[ok] dropped {collection.name}.{name}")

    for spec in specs:
        kwargs: Dict[str, Any] = {"name": spec.name}
        if spec.unique:
            kwargs["unique"] = True

        if dry_run:
            print(
                f"[dry-run] create {collection.name}.{spec.name} "
                f"keys={list(spec.keys)} unique={spec.unique}"
            )
            continue

        collection.create_index(list(spec.keys), **kwargs)
        print(
            f"[ok] ensured {collection.name}.{spec.name} "
            f"keys={list(spec.keys)} unique={spec.unique}"
        )


def main() -> None:
    args = parse_args()
    if not args.mongo_uri:
        raise ValueError("Mongo URI missing. Set --mongo-uri or MONGO_URI.")
    if not args.mongo_db:
        raise ValueError("Mongo DB missing. Set --mongo-db or MONGO_DB.")

    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]

    print(
        f"Applying indexes on db='{args.mongo_db}' "
        f"(drop_extra={args.drop_extra}, dry_run={args.dry_run})"
    )
    errors = 0
    for collection_name, specs in _specs().items():
        print(f"\nCollection: {collection_name}")
        try:
            _ensure_indexes(
                collection=db[collection_name],
                specs=specs,
                drop_extra=bool(args.drop_extra),
                dry_run=bool(args.dry_run),
            )
        except PyMongoError as exc:
            errors += 1
            print(f"[error] {collection_name}: {type(exc).__name__}: {exc}")

    if errors:
        raise SystemExit(f"Completed with {errors} collection error(s).")
    print("\nDone.")


if __name__ == "__main__":
    main()
