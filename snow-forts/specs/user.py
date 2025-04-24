"""User specification definitions."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any


class UserType(Enum):
    """Type of Snowflake user."""
    REGULAR = "regular"
    SERVICE_ACCOUNT = "service_account"


@dataclass
class UserSpec:
    """Specification for a Snowflake user."""

    name: str
    role: str
    user_type: UserType
    comment: Optional[str] = None
    secret_name: Optional[str] = None
    prefix_with_environment: bool = True
    rsa_public_key: Optional[str] = None
    rsa_public_key_2: Optional[str] = None  # For key rotation
    default_warehouse: Optional[str] = None
    default_namespace: Optional[str] = None
    default_role: Optional[str] = None
    must_change_password: bool = True  # Only applies to regular users
    disabled: bool = False
    custom_attributes: Optional[Dict[str, Any]] = None
