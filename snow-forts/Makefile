.PHONY: test clean

test:
	python -m pytest -xvs tests/unit/

clean:
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	find . -name "*.pyd" -delete
	find . -name ".pytest_cache" -type d -exec rm -rf {} +
	find . -name "*.egg-info" -type d -exec rm -rf {} +
	find . -name "*.egg" -delete 