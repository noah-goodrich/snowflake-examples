"""User service implementation."""

from typing import Any, Dict, Optional
from snowflake.core import Root
from snowflake.core.user import User
from specs.user import UserSpec, UserType
from services.base import Service
from services.key_management import KeyManagementService


class UserService(Service):
    """Service for executing user operations."""

    def __init__(self, snow: Root, key_management: KeyManagementService):
        """Initialize the user service.

        Args:
            snow: Snowflake connection
            key_management: Key management service
        """
        self.snow = snow
        self.key_management = key_management

    def create(self, spec: UserSpec) -> Dict[str, Any]:
        """Create a user.

        Args:
            spec: The user specification

        Returns:
            Dictionary containing operation results
        """
        try:
            # Create base user object
            user = User(
                name=spec.name,
                comment=spec.comment,
                default_warehouse=spec.default_warehouse,
                default_namespace=spec.default_namespace,
                default_role=spec.default_role,
                disabled=spec.disabled
            )

            # Set must change password for regular users
            if spec.must_change_password and spec.user_type == UserType.REGULAR:
                user.must_change_password = True

            # Create the user
            created_user = self.snow.users.create(user)

            # Handle service account keys
            if spec.user_type == UserType.SERVICE_ACCOUNT:
                if spec.rsa_public_key:
                    # Validate existing key pair if provided
                    if not self.key_management.validate_key_pair(spec.name, spec.rsa_public_key):
                        raise ValueError("Invalid key pair provided")
                else:
                    # Generate new key pair
                    public_key, _ = self.key_management.generate_key_pair(
                        spec.name)
                    created_user.rsa_public_key = public_key
                    created_user.save()

            # Grant role if specified
            if spec.role:
                role = self.snow.roles[spec.role].fetch()
                role.grant_to(created_user)

            return {
                "name": created_user.name,
                "user_type": spec.user_type.value,
                "comment": created_user.comment,
                "default_warehouse": created_user.default_warehouse,
                "default_namespace": created_user.default_namespace,
                "default_role": created_user.default_role,
                "disabled": created_user.disabled,
                "role": spec.role,
                "must_change_password": created_user.must_change_password
            }

        except Exception as e:
            return {"error": str(e)}

    def alter(self, spec: UserSpec) -> Dict[str, Any]:
        """Alter a user.

        Args:
            spec: The user specification

        Returns:
            Dictionary containing operation results
        """
        try:
            # Get current user
            user = self.snow.users[spec.name].fetch()
            changes = []

            # Update basic properties
            for attr in ["comment", "default_warehouse", "default_namespace", "default_role", "disabled"]:
                new_value = getattr(spec, attr)
                if new_value != getattr(user, attr):
                    setattr(user, attr, new_value)
                    changes.append(attr)

            # Handle key rotation for service accounts
            if (spec.user_type == UserType.SERVICE_ACCOUNT and
                spec.rsa_public_key and
                    spec.rsa_public_key != user.rsa_public_key):
                # Validate new key pair
                if not self.key_management.validate_key_pair(spec.name, spec.rsa_public_key):
                    # Generate new key pair
                    public_key, _ = self.key_management.rotate_keys(spec.name)
                    user.rsa_public_key = public_key
                    changes.append("rsa_public_key")

            # Save changes if any
            if changes:
                user.save()

            # Update role if needed
            if spec.role and spec.role != user.default_role:
                # Grant new role
                role = self.snow.roles[spec.role].fetch()
                role.grant_to(user)
                changes.append("role")

            return {
                "name": user.name,
                "changes": changes,
                "role": spec.role
            }

        except Exception as e:
            return {"error": str(e)}

    def drop(self, spec: UserSpec) -> Dict[str, Any]:
        """Drop a user.

        Args:
            spec: The user specification

        Returns:
            Dictionary containing operation results
        """
        try:
            user = self.snow.users[spec.name].fetch()
            user.delete()

            # Clean up service account keys
            if spec.user_type == UserType.SERVICE_ACCOUNT:
                self.key_management.delete_private_key(spec.name)

            return {"name": spec.name}
        except Exception as e:
            return {"error": str(e)}
