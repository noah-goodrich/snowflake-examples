"""Tests for base state management classes."""

import pytest
from datetime import datetime
from typing import Dict, Any, Optional
from unittest.mock import MagicMock, patch

from resources.state.base import (
    Operation,
    ResourceStateChange,
    StateChangeMetadata,
    OperationType
)
from resources.state.types import OperationType, StateChangeMetadata


class TestOperation(Operation[str, str]):
    """Test operation class."""

    def __init__(self, resource: str, config: str, metadata: StateChangeMetadata, previous_state: Optional[Dict[str, Any]] = None):
        """Initialize test operation."""
        super().__init__(resource, config, metadata, previous_state)

    def execute(self) -> None:
        """Execute the operation."""
        pass

    def apply(self) -> None:
        """Apply the operation."""
        pass

    def validate(self) -> bool:
        """Validate the operation."""
        return True

    def rollback(self) -> None:
        """Rollback the operation."""
        pass


class TestResourceStateChange(ResourceStateChange[str, str]):
    """Test resource state change class."""

    def __init__(self, resource: MagicMock):
        """Initialize test resource state change."""
        super().__init__(resource)

    def check_current_state(self, resource: str) -> dict:
        """Check current state."""
        return {}

    def validate_state(self, state: str) -> bool:
        """Validate state."""
        return True

    def create_operation(self, state: str) -> Operation[str, str]:
        """Create operation."""
        return TestOperation(
            resource="test_resource",
            config=state,
            metadata=StateChangeMetadata(
                identifier="test",
                timestamp=datetime.now(),
                description="Test operation"
            )
        )

    def alter_operation(self, current_state: dict, desired_state: str) -> Operation[str, str]:
        """Create alter operation."""
        return TestOperation(
            resource="test_resource",
            config=desired_state,
            metadata=StateChangeMetadata(
                identifier="test",
                timestamp=datetime.now(),
                description="Test operation"
            ),
            previous_state=current_state
        )

    def drop_operation(self, current_state: dict) -> Operation[str, str]:
        """Create drop operation."""
        return TestOperation(
            resource="test_resource",
            config=None,
            metadata=StateChangeMetadata(
                identifier="test",
                timestamp=datetime.now(),
                description="Test operation"
            ),
            previous_state=current_state
        )

    def get_operation(self, resource: str, config: str, metadata: StateChangeMetadata) -> Operation[str, str]:
        """Get appropriate operation."""
        return self.create_operation(config)


def test_operation_execution():
    """Test operation execution."""
    operation = TestOperation(
        resource="test_resource",
        config="test_config",
        metadata=StateChangeMetadata(
            identifier="test",
            timestamp=datetime.now(),
            description="Test operation"
        )
    )
    operation.execute()


def test_resource_state_change():
    """Test resource state change."""
    state_change = TestResourceStateChange(MagicMock())
    assert state_change.check_current_state("test") == {}
    assert state_change.validate_state("test") is True

    operation = state_change.create_operation("test")
    assert operation.config == "test"

    operation = state_change.alter_operation({}, "new")
    assert operation.config == "new"
    assert operation.previous_state == {}

    operation = state_change.drop_operation({})
    assert operation.config is None
    assert operation.previous_state == {}


def test_state_change_metadata():
    """Test state change metadata."""
    identifier = "test_id"
    description = "test description"
    dependencies = ["dep1", "dep2"]
    timestamp = datetime.now()

    metadata = StateChangeMetadata(
        identifier=identifier,
        description=description,
        dependencies=dependencies,
        timestamp=timestamp
    )

    assert metadata.identifier == identifier
    assert metadata.description == description
    assert metadata.dependencies == dependencies
    assert metadata.timestamp == timestamp


def test_resource_state_change_initialization():
    """Test resource state change initialization."""
    mock_snow = MagicMock()
    state_change = TestResourceStateChange(mock_snow)
    assert state_change.persistence is not None


def test_resource_state_change_get_differences():
    """Test getting differences between states."""
    mock_snow = MagicMock()
    state_change = TestResourceStateChange(mock_snow)

    # Test with no current state
    current_state = None
    new_state = {"value": "new_value"}
    differences = state_change.get_differences(current_state, new_state)
    assert differences == new_state

    # Test with different values
    current_state = {"value": "old_value"}
    new_state = {"value": "new_value"}
    differences = state_change.get_differences(current_state, new_state)
    assert differences == new_state

    # Test with same values
    current_state = {"value": "same_value"}
    new_state = {"value": "same_value"}
    differences = state_change.get_differences(current_state, new_state)
    assert differences == {}


@patch('resources.state.base.StatePersistence')
def test_resource_state_change_apply(mock_persistence):
    """Test applying state with persistence."""
    mock_snow = MagicMock()
    state_change = TestResourceStateChange(mock_snow)
    resource = "test_resource"
    config = "test_config"
    metadata = StateChangeMetadata("test_id", "test description")

    # Create a mock instance and inject it into the state change
    mock_persistence_instance = MagicMock()
    mock_persistence.return_value = mock_persistence_instance
    state_change.persistence = mock_persistence_instance

    # Test successful state application
    state_change.apply(resource, config, metadata)
    mock_persistence_instance.record_state.assert_called_once()
    mock_persistence_instance.update_state_status.assert_called_with(
        mock_persistence_instance.record_state.call_args[0][0].change_id,
        "completed"
    )

    # Test failed state application
    mock_persistence_instance.record_state.side_effect = Exception(
        "Test error")
    with pytest.raises(Exception):
        state_change.apply(resource, config, metadata)
    mock_persistence_instance.update_state_status.assert_called_with(
        mock_persistence_instance.record_state.call_args[0][0].change_id,
        "failed"
    )
