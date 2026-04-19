# Testing Conventions

## Framework
- `pytest` + `pytest-asyncio` for all tests.
- Mark async tests with `@pytest.mark.asyncio`.
- Configure in `pyproject.toml`: `asyncio_mode = "auto"` so all async tests run automatically.

## Directory Structure
```
tests/
├── conftest.py          # shared fixtures (mock client, tmp paths)
├── test_client.py       # unit tests for client.py
├── test_models.py       # unit tests for models.py
├── test_fetchers_markets.py
├── test_fetchers_price_history.py
├── test_fetchers_prices.py
├── test_storage.py      # unit tests for parquet.py and state.py
└── test_integration.py  # end-to-end tests (mocked network)
```

## Mocking
- Mock at the `client.get()` boundary using `unittest.mock.AsyncMock` or `pytest-mock`.
- **Never** mock at a lower level (e.g., `httpx.AsyncClient`). Mock the public `get()` function.
- **Never** make real network calls in unit or integration tests.
- Fixture pattern:
  ```python
  @pytest.fixture
  def mock_get(mocker):
      return mocker.patch("polymarket.client.get", new_callable=AsyncMock)
  ```

## What to Test per Fetcher
1. **Happy path**: valid API response → correct model instances returned.
2. **Malformed JSON**: missing required field → record skipped, error logged, no exception raised.
3. **Empty response**: empty list / no `history` key → returns empty list, no exception.
4. **Pagination**: multiple pages → all pages fetched, state saved after each.
5. **Idempotency**: re-running with saved state → starts from last cursor, no duplicates.

## Storage Tests
- Use `tmp_path` pytest fixture for all file writes — never write to `data/` in tests.
- Verify Parquet schema (column names, dtypes) after write.
- Verify state JSON round-trips correctly (write then read back).

## Integration Tests
- Use `respx` (httpx mock library) to intercept requests at the httpx level for integration tests.
- Test the full path: fetcher → model validation → write → verify file on disk.
- Dry-run mode: verify no files written when `dry_run=True`.
