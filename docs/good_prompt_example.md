Context: We're building the Gamma API market fetcher (src/polymarket/fetchers/markets.py).

Task: Implement cursor-based pagination against GET https://gamma-api.polymarket.com/markets.
Each response returns a list of markets and an `offset` for the next page. 
Persist the last offset to data/state/markets.json after each page.
Stop when the response returns fewer than `limit` items.

Use the existing client in client.py. Validate each market against the 
GammaMarket Pydantic model. Log every page fetched and every validation error.