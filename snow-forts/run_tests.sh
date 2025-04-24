#!/bin/bash

# Run unit tests with coverage
python -m pytest -xvs tests/unit/ --cov=services --cov=state_managers --cov=specs 