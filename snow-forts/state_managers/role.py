"""Role state manager implementation."""

from typing import Any, Dict, List, Optional

from snowflake.core import Root
from specs.role import RoleSpec
from state_managers.types import StateChangeMetadata

from .base import StateManager


class RoleStateManager(StateManager[RoleSpec]):
    """Manages role state operations."""

    def __init__(self, snow: Root):
        """Initialize the role state manager.

        Args:
            snow: Snowflake connection
        """
        self.snow = snow

    def apply(self, spec: RoleSpec) -> Dict[str, Any]:
        """Apply the desired state for a role.

        Args:
            spec: The role specification to apply

        Returns:
            Dictionary containing operation results
        """
        # Create metadata for the operation
        metadata = StateChangeMetadata(
            identifier=spec.name,
            description=f"Applying state for role {spec.name}"
        )

        # Get current state
        current_state = self._get_current_state(spec.name)

        # Determine required operations
        operations = self._determine_operations(current_state, spec)

        # Execute operations
        results = {}
        for op in operations:
            results[op] = self._execute_operation(op, spec, metadata)

        return results

    def _get_current_state(self, role_name: str) -> Dict[str, Any]:
        """Get the current state of a role.

        Args:
            role_name: Name of the role to get state for

        Returns:
            Dictionary containing current state
        """
        try:
            # Check if role exists
            role = self.snow.roles[role_name].fetch()

            # Get granted roles
            granted_roles = []
            for grant in self.snow.session.sql(f"SHOW GRANTS TO ROLE {role_name}").collect():
                if grant['privilege'] == 'USAGE' and grant['granted_on'] == 'ROLE':
                    granted_roles.append(grant['name'])

            # Get roles granted to this role
            granted_to = []
            for grant in self.snow.session.sql(f"SHOW GRANTS OF ROLE {role_name}").collect():
                if grant['grantee_type'] == 'ROLE':
                    granted_to.append(grant['grantee_name'])

            return {
                'exists': True,
                'comment': role.comment,
                'granted_roles': granted_roles,
                'granted_to': granted_to
            }
        except Exception:
            return {
                'exists': False,
                'comment': None,
                'granted_roles': [],
                'granted_to': []
            }

    def _determine_operations(self, current: Dict[str, Any], desired: RoleSpec) -> List[str]:
        """Determine required operations to achieve desired state.

        Args:
            current: Current state
            desired: Desired role specification

        Returns:
            List of required operations
        """
        operations = []

        if not current['exists']:
            operations.append('CREATE')
        else:
            if current['comment'] != desired.comment:
                operations.append('ALTER')
            if set(current['granted_roles']) != set(desired.granted_roles or []):
                operations.append('GRANT_ROLES')
            if set(current['granted_to']) != set(desired.granted_to or []):
                operations.append('GRANT_TO')

        return operations

    def _execute_operation(self, operation: str, spec: RoleSpec, metadata: StateChangeMetadata) -> Dict[str, Any]:
        """Execute a specific operation.

        Args:
            operation: Operation to execute
            spec: Role specification
            metadata: Operation metadata

        Returns:
            Dictionary containing operation results
        """
        try:
            if operation == 'CREATE':
                # Create the role
                role = self.snow.roles.create(spec.name)
                if spec.comment:
                    role.comment = spec.comment
                    role.save()
                return {'status': 'success', 'operation': 'CREATE'}

            elif operation == 'ALTER':
                # Update role properties
                role = self.snow.roles[spec.name].fetch()
                if spec.comment:
                    role.comment = spec.comment
                    role.save()
                return {'status': 'success', 'operation': 'ALTER'}

            elif operation == 'GRANT_ROLES':
                # Grant roles to this role
                role = self.snow.roles[spec.name].fetch()
                for granted_role in spec.granted_roles or []:
                    self.snow.session.sql(
                        f"GRANT ROLE {granted_role} TO ROLE {spec.name}").collect()
                return {'status': 'success', 'operation': 'GRANT_ROLES'}

            elif operation == 'GRANT_TO':
                # Grant this role to other roles
                for granted_to in spec.granted_to or []:
                    self.snow.session.sql(
                        f"GRANT ROLE {spec.name} TO ROLE {granted_to}").collect()
                return {'status': 'success', 'operation': 'GRANT_TO'}

            return {'status': 'error', 'message': f'Unknown operation: {operation}'}

        except Exception as e:
            return {'status': 'error', 'message': str(e)}
