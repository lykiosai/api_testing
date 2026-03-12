# API Testing — Quick Start

## Overview
This repo contains a minimal Python setup for testing an API (example: RapidAPI endpoints). The main test script is `test_api.py` and sample request/response fixtures are under `rapidapi_test/rapidapi_candles.json`.

## Prerequisites
- Python 3.8+ installed
- A Python virtual environment (recommended)
- API key(s) for the target API (store securely)

## Setup
1. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration
- Add your API key as an environment variable (.env file):

```bash
export API_KEY="your_api_key_here"
export HOST_NAME="your_host_name_here"
```

- The repository includes `rapidapi_test/rapidapi_candles.json` as an example request/response fixture. Update or add JSON fixtures for other endpoints as needed.

## Running the tests / script
- Run the main test script directly:

```bash
python test_api.py
```

- If you add unit tests (e.g., using `pytest`), run:

```bash
pytest -q
```

## How the testing flow works
- `test_api.py` contains the logic that sends requests to the API using the configured API key and compares responses to expected fixtures (JSON files).
- Fixtures live in `rapidapi_test/` and can be used to validate response schemas, field values, and edge-case behaviors.
- Tests should fail when responses diverge from expected output; update fixtures only when the API contract intentionally changes.

## Adding new tests
- Add a new fixture JSON under `rapidapi_test/` for the endpoint and scenario you want to validate.
- Add a test function in `test_api.py` (or a new test file) that loads the fixture, sends the request, and asserts expected fields.

## Tips and best practices
- Do not commit secrets; keep keys out of the repo and add them to `.gitignore` (already present).
- Use small, focused tests for clarity and fast feedback.
- Record flaky tests and add retries or mocks for external instability.

## Troubleshooting
- "Module not found" — ensure the virtualenv is active and `requirements.txt` is installed.
- "Authentication errors" — verify `API_KEY` env var is set and valid.


email: rhs0@yahoo.com
