from typing import Any, Dict, Optional

from app.models.user import User
from app.utils.houses_utils import seller_id_from_house_doc


def ensure_roles(current_user: User, allowed_roles: set[str], message: Optional[str] = None) -> None:
    role = str(current_user.role).strip().lower()
    normalized_allowed = {str(item).strip().lower() for item in allowed_roles}
    if role not in normalized_allowed:
        if message:
            raise PermissionError(message)
        raise PermissionError(f"Role '{role}' is not allowed to perform this operation.")


def ensure_admin(current_user: User) -> None:
    role = str(current_user.role).strip().lower()
    if role != "admin":
        raise PermissionError("Admin role required to perform this operation.")


def ensure_can_create_house(current_user: User) -> None:
    role = str(current_user.role).strip().lower()
    if role not in {"admin", "seller"}:
        raise PermissionError("Not allowed to create houses.")


def is_house_owned_by_seller(current_user: User, house_doc: Dict[str, Any]) -> bool:
    seller_id = seller_id_from_house_doc(house_doc)
    return seller_id is not None and str(seller_id) == str(current_user.id)


def ensure_can_manage_house(current_user: User, house_doc: Dict[str, Any]) -> None:
    role = str(current_user.role).strip().lower()
    if role == "admin":
        return
    if role == "seller" and is_house_owned_by_seller(current_user, house_doc):
        return
    raise PermissionError("Not allowed to modify this house.")


def booking_buyer_id(booking_doc: Dict[str, Any]) -> Optional[str]:
    direct_id = booking_doc.get("buyer_id")
    if direct_id is not None:
        return str(direct_id)

    buyer = booking_doc.get("buyer")
    if isinstance(buyer, dict):
        nested_id = buyer.get("id") or buyer.get("_id")
        if nested_id is not None:
            return str(nested_id)

    return None


def booking_seller_id(booking_doc: Dict[str, Any]) -> Optional[str]:
    direct_id = booking_doc.get("seller_id")
    if direct_id is not None:
        return str(direct_id)

    seller = booking_doc.get("seller")
    if isinstance(seller, dict):
        nested_id = seller.get("id") or seller.get("_id")
        if nested_id is not None:
            return str(nested_id)

    return None


def can_view_booking(current_user: User, booking_doc: Dict[str, Any]) -> bool:
    role = str(current_user.role).strip().lower()
    current_user_id = str(current_user.id)

    if role == "admin":
        return True
    return (
        booking_buyer_id(booking_doc) == current_user_id
        or booking_seller_id(booking_doc) == current_user_id
    )


def ensure_can_create_booking(current_user: User) -> None:
    role = str(current_user.role).strip().lower()
    if role not in {"buyer", "seller", "admin"}:
        raise PermissionError("Only buyers, sellers, and admins can create bookings.")


def ensure_can_modify_booking(current_user: User, booking_doc: Dict[str, Any]) -> None:
    if can_view_booking(current_user, booking_doc):
        return
    raise PermissionError("Not allowed to modify this booking.")


def callslot_buyer_id(callslot_doc: Dict[str, Any]) -> Optional[str]:
    direct_id = callslot_doc.get("buyer_id")
    if direct_id is not None:
        return str(direct_id)

    buyer = callslot_doc.get("buyer")
    if isinstance(buyer, dict):
        nested_id = buyer.get("id") or buyer.get("_id")
        if nested_id is not None:
            return str(nested_id)

    return None


def callslot_seller_id(callslot_doc: Dict[str, Any]) -> Optional[str]:
    direct_id = callslot_doc.get("seller_id")
    if direct_id is not None:
        return str(direct_id)

    seller = callslot_doc.get("seller")
    if isinstance(seller, dict):
        nested_id = seller.get("id") or seller.get("_id")
        if nested_id is not None:
            return str(nested_id)

    return None


def can_view_callslot(current_user: User, callslot_doc: Dict[str, Any]) -> bool:
    role = str(current_user.role).strip().lower()
    current_user_id = str(current_user.id)

    if role == "admin":
        return True
    return (
        callslot_buyer_id(callslot_doc) == current_user_id
        or callslot_seller_id(callslot_doc) == current_user_id
    )


def ensure_can_create_callslot(current_user: User) -> None:
    role = str(current_user.role).strip().lower()
    if role not in {"buyer", "seller", "admin"}:
        raise PermissionError("Not allowed to create callslots.")


def ensure_can_modify_callslot(current_user: User, callslot_doc: Dict[str, Any]) -> None:
    if can_view_callslot(current_user, callslot_doc):
        return
    raise PermissionError("Not allowed to modify this callslot.")
