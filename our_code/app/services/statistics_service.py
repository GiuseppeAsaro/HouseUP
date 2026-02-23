from typing import Any, Dict, List

from app.core.database import mongo_db, redis_client
from app.core.database import get_redis_node_role
from app.models.statistics import (
    BookingOutcomeStatRow,
    FeedbackSatisfactionStatRow,
    HousingInventoryResponse,
    HousingInventoryStatRow,
)
from app.models.user import User
from app.utils.cache_utils import cache_get, cache_note, cache_set
from app.utils.permissions_utils import ensure_admin


_BOOKING_RESULTS_COLLECTION = mongo_db["booking_results"]
_FEEDBACKS_COLLECTION = mongo_db["feedbacks"]
_HOUSES_COLLECTION = mongo_db["houses"]
_HOUSING_INVENTORY_CACHE_KEY = "stats:housing_inventory:v1"
_HOUSING_INVENTORY_CACHE_TTL_SECONDS = 300


def get_booking_outcome_stats(
    current_user: User,
) -> List[BookingOutcomeStatRow]:
    ensure_admin(current_user)

    pipeline: List[Dict[str, Any]] = [
        {
            "$project": {
                "period": {
                    "$ifNull": [
                        {
                            "$dateToString": {
                                "format": "%Y-%m",
                                "date": "$booking_date",
                            }
                        },
                        "unknown",
                    ]
                },
                "price_num": {
                    "$convert": {
                        "input": "$final_price",
                        "to": "double",
                        "onError": None,
                        "onNull": None,
                    }
                },
                "listing_price_num": {
                    "$convert": {
                        "input": "$house.listing_price",
                        "to": "double",
                        "onError": None,
                        "onNull": None,
                    }
                },
                "outcome": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$house_bought", True]}, "then": "purchase_completed"},
                            {"case": {"$eq": ["$house_bought", False]}, "then": "visit_only"},
                        ],
                        "default": "unknown",
                    }
                },
                "booking_weight": {"$cond": [{"$eq": ["$house_bought", True]}, 2, 1]},
                "has_valid_listing": {
                    "$let": {
                        "vars": {
                            "price_num": {
                                "$convert": {
                                    "input": "$final_price",
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "listing_price_num": {
                                "$convert": {
                                    "input": "$house.listing_price",
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                        },
                        "in": {
                            "$and": [
                                {"$ne": ["$$listing_price_num", None]},
                                {"$gt": ["$$listing_price_num", 0]},
                                {"$ne": ["$$price_num", None]},
                            ]
                        },
                    }
                },
                "price_delta": {
                    "$let": {
                        "vars": {
                            "price_num": {
                                "$convert": {
                                    "input": "$final_price",
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "listing_price_num": {
                                "$convert": {
                                    "input": "$house.listing_price",
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                        },
                        "in": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$$listing_price_num", None]},
                                        {"$gt": ["$$listing_price_num", 0]},
                                        {"$ne": ["$$price_num", None]},
                                    ]
                                },
                                {"$subtract": ["$$price_num", "$$listing_price_num"]},
                                None,
                            ]
                        },
                    }
                },
                "price_delta_pct": {
                    "$let": {
                        "vars": {
                            "price_num": {
                                "$convert": {
                                    "input": "$final_price",
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "listing_price_num": {
                                "$convert": {
                                    "input": "$house.listing_price",
                                    "to": "double",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                        },
                        "in": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$$listing_price_num", None]},
                                        {"$gt": ["$$listing_price_num", 0]},
                                        {"$ne": ["$$price_num", None]},
                                    ]
                                },
                                {
                                    "$multiply": [
                                        {
                                            "$divide": [
                                                {"$subtract": ["$$price_num", "$$listing_price_num"]},
                                                "$$listing_price_num",
                                            ]
                                        },
                                        100,
                                    ]
                                },
                                None,
                            ]
                        },
                    }
                },
            }
        },
        {
            "$group": {
                "_id": {"period": "$period", "outcome": "$outcome"},
                "total_bookings": {"$sum": 1},
                "average_price": {"$avg": "$price_num"},
                "min_price": {"$min": "$price_num"},
                "max_price": {"$max": "$price_num"},
                "total_weighted_impact": {"$sum": "$booking_weight"},
                "delta_sample_size": {"$sum": {"$cond": ["$has_valid_listing", 1, 0]}},
                "average_listing_price": {
                    "$avg": {"$cond": ["$has_valid_listing", "$listing_price_num", None]}
                },
                "average_price_delta": {"$avg": {"$cond": ["$has_valid_listing", "$price_delta", None]}},
                "average_price_delta_pct": {
                    "$avg": {"$cond": ["$has_valid_listing", "$price_delta_pct", None]}
                },
            }
        },
        {"$sort": {"_id.period": 1, "total_weighted_impact": -1}},
        {
            "$project": {
                "_id": 0,
                "period": "$_id.period",
                "outcome": "$_id.outcome",
                "total_bookings": 1,
                "weighted_score": "$total_weighted_impact",
                "paired_sample_size": "$delta_sample_size",
                "final_price_avg": "$average_price",
                "final_price_min": "$min_price",
                "final_price_max": "$max_price",
                "listing_price_avg": "$average_listing_price",
                "final_vs_listing_amount": {
                    "$cond": [
                        {"$ne": ["$average_price_delta", None]},
                        {"$abs": "$average_price_delta"},
                        None,
                    ]
                },
                "final_vs_listing_pct": {
                    "$cond": [
                        {"$ne": ["$average_price_delta_pct", None]},
                        {"$abs": "$average_price_delta_pct"},
                        None,
                    ]
                },
                "final_vs_listing_direction": {
                    "$let": {
                        "vars": {"delta_pct": "$average_price_delta_pct"},
                        "in": {
                            "$cond": [
                                {"$eq": ["$$delta_pct", None]},
                                "$$REMOVE",
                                {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {
                                                    "$lt": [
                                                        {"$abs": "$$delta_pct"},
                                                        0.000000001,
                                                    ]
                                                },
                                                "then": "equal",
                                            },
                                            {"case": {"$lt": ["$$delta_pct", 0]}, "then": "below_listing"},
                                            {"case": {"$gt": ["$$delta_pct", 0]}, "then": "above_listing"},
                                        ],
                                        "default": "$$REMOVE",
                                    }
                                },
                            ]
                        },
                    }
                },
            }
        },
    ]

    try:
        rows = list(_BOOKING_RESULTS_COLLECTION.aggregate(pipeline))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to compute booking outcomes statistics.") from exc

    return [BookingOutcomeStatRow.from_json(row) for row in rows]


def get_feedback_satisfaction_stats(
    current_user: User,
) -> List[FeedbackSatisfactionStatRow]:
    ensure_admin(current_user)

    pipeline: List[Dict[str, Any]] = [
        {
            "$project": {
                "period": {
                    "$ifNull": [
                        {
                            "$dateToString": {
                                "format": "%Y-%m",
                                "date": {
                                    "$convert": {
                                        "input": "$feedback_date",
                                        "to": "date",
                                        "onError": None,
                                        "onNull": None,
                                    }
                                },
                            }
                        },
                        "unknown",
                    ]
                },
                "rating_num": {
                    "$convert": {
                        "input": "$rating",
                        "to": "int",
                        "onError": None,
                        "onNull": None,
                    }
                },
                "category": {
                    "$let": {
                        "vars": {
                            "rating_num": {
                                "$convert": {
                                    "input": "$rating",
                                    "to": "int",
                                    "onError": None,
                                    "onNull": None,
                                }
                            }
                        },
                        "in": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$lte": ["$$rating_num", 2]}, "then": "Negative (1-2)"},
                                    {"case": {"$eq": ["$$rating_num", 3]}, "then": "Neutral (3)"},
                                    {"case": {"$gte": ["$$rating_num", 4]}, "then": "Positive (4-5)"},
                                ],
                                "default": "Invalid",
                            }
                        },
                    }
                },
                "has_comment": {
                    "$cond": [
                        {"$and": [{"$ne": ["$comment", None]}, {"$ne": ["$comment", ""]}]},
                        1,
                        0,
                    ]
                },
                "feedback_weight": {
                    "$cond": [
                        {"$and": [{"$ne": ["$comment", None]}, {"$ne": ["$comment", ""]}]},
                        2,
                        1,
                    ]
                },
            }
        },
        {
            "$group": {
                "_id": {"period": "$period", "category": "$category"},
                "total_feedback": {"$sum": 1},
                "feedback_with_comment": {"$sum": "$has_comment"},
                "feedback_without_comment": {
                    "$sum": {"$cond": [{"$eq": ["$has_comment", 0]}, 1, 0]}
                },
                "average_rating": {"$avg": "$rating_num"},
                "total_weighted_score": {"$sum": "$feedback_weight"},
            }
        },
        {"$sort": {"_id.period": 1, "total_weighted_score": -1}},
        {
            "$project": {
                "_id": 0,
                "period": "$_id.period",
                "category": "$_id.category",
                "total_feedback": 1,
                "feedback_with_comment": 1,
                "feedback_without_comment": 1,
                "average_rating": 1,
                "total_weighted_score": 1,
            }
        },
    ]

    try:
        rows = list(_FEEDBACKS_COLLECTION.aggregate(pipeline))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to compute feedback satisfaction statistics.") from exc

    return [FeedbackSatisfactionStatRow.from_json(row) for row in rows]


def get_housing_inventory_stats(
    current_user: User,
) -> HousingInventoryResponse:
    ensure_admin(current_user)

    cached = cache_get(redis_client, _HOUSING_INVENTORY_CACHE_KEY)
    if cached is not None:
        note_payload = cache_note(True, True, node_role=get_redis_node_role())
        data = [HousingInventoryStatRow.from_json(row) for row in cached]
        return HousingInventoryResponse(data=data, note=(note_payload or {}).get("note"))

    try:
        pipeline = [
            {
                "$project": {
                    "price": 1,
                    "is_sold": 1,
                    "price_category": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {
                                        "$or": [
                                            {"$lt": ["$price", 100000]},
                                            {"$eq": ["$price", None]},
                                        ]
                                    },
                                    "then": "Budget",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["$price", 100000]},
                                            {"$lte": ["$price", 300000]},
                                        ]
                                    },
                                    "then": "Mid-Range",
                                },
                                {
                                    "case": {"$gt": ["$price", 300000]},
                                    "then": "Luxury",
                                },
                            ],
                            "default": "Budget",
                        }
                    },
                }
            },
            {
                "$group": {
                    "_id": "$price_category",
                    "total_houses": {"$sum": 1},
                    "available_houses": {"$sum": {"$cond": [{"$eq": ["$is_sold", False]}, 1, 0]}},
                    "unavailable_houses": {"$sum": {"$cond": [{"$eq": ["$is_sold", False]}, 0, 1]}},
                    "average_price": {"$avg": "$price"},
                    "min_price": {"$min": "$price"},
                    "max_price": {"$max": "$price"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "price_category": "$_id",
                    "total_houses": 1,
                    "available_houses": 1,
                    "unavailable_houses": 1,
                    "average_price": 1,
                    "min_price": 1,
                    "max_price": 1,
                }
            },
            {"$sort": {"total_houses": -1}},
        ]
        rows = list(_HOUSES_COLLECTION.aggregate(pipeline))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to compute housing inventory statistics.") from exc

    data = [HousingInventoryStatRow.from_json(row) for row in rows]

    cache_set(
        redis_client,
        _HOUSING_INVENTORY_CACHE_KEY,
        [item.to_json() for item in data],
        _HOUSING_INVENTORY_CACHE_TTL_SECONDS,
    )
    return HousingInventoryResponse(data=data, note="computed from mongo")
