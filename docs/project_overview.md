Technical Architecture and Quantitative Implementation for Extracting NBA Win Probabilities from Polymarket Infrastructure
The intersection of decentralized finance and sports analytics has reached a critical inflection point with the rise of prediction markets like Polymarket. For quantitative researchers and developers seeking to extract high-fidelity win probabilities for NBA games, Polymarket represents more than a wagering platform; it is a sophisticated, high-velocity price discovery engine. Unlike traditional sportsbooks that rely on centralized oddsmakers and proprietary models, Polymarket utilizes a peer-to-peer Central Limit Order Book (CLOB) on the Polygon blockchain to determine the likelihood of outcomes through the collective intelligence of market participants. The extraction of historical win probabilities from this infrastructure requires a deep understanding of its multi-tiered API architecture, the underlying cryptographic primitives, and the specific data structures used to represent sports events.

The Architectural Paradigm of Polymarket Data Services
To programmatically retrieve data such as the win probability over time for a specific game, a researcher must navigate three distinct yet interconnected API services. Each service handles a different domain of the market lifecycle, and understanding their separation of concerns is fundamental to building a reliable data pipeline.

The Gamma API: Discovery and Metadata Layer
The Gamma API serves as the primary gateway for market discovery and metadata retrieval. It acts as an indexing service that mirrors on-chain state while enriching it with human-readable information, such as team names, event categories, and hierarchical groupings. When a developer starts with a game slug like nba-atl-cha-2026-02-11, the Gamma API is the tool used to translate this string into the machine-readable identifiers required for pricing data. It provides access to the "Events" and "Markets" objects, where an Event represents the overarching game and a Market represents a specific tradable outcome within that game, such as the Moneyline or the Point Spread.   

The CLOB API: Transactional and Historical Pricing
The Central Limit Order Book (CLOB) API is the engine for live market dynamics and historical price series. While Gamma provides the "metadata," the CLOB API provides the "market data". This service manages the order book, tracks every trade execution, and provides endpoints for historical price intervals. For the purpose of extracting win probability over time, the CLOB API's prices-history endpoint is the most critical resource, as it allows for the retrieval of time-stamped price points that represent the market's evolving consensus on an outcome's probability.   

The Data API: Post-Trade Analytics
The Data API focuses on historical activity and analytical metrics that are not directly required for trade execution but are vital for market research. This includes data on user positions, open interest, holder distributions, and broader volume trends. When analyzing the "conviction" behind a specific probability move in an NBA game, the Data API allows researchers to correlate price shifts with changes in open interest or the activity of large-scale participants, often referred to as "whales".   

API Layer	Base URL	Primary Utility	Authentication Requirement
Gamma API	https://gamma-api.polymarket.com	Metadata, Slugs, Tags, Discovery	Public / No Auth
CLOB API	https://clob.polymarket.com	Order Books, Trades, Price History	Public (Read) / L1/L2 (Write)
Data API	https://data-api.polymarket.com	Positions, Open Interest, Analytics	Public / No Auth
The NBA Market Model: Hierarchy and Resolution
NBA games on Polymarket are structured using the Gnosis Conditional Token Framework (CTF), which facilitates the creation of outcome tokens that represent a $1.00 claim upon the occurrence of a specific event. To extract win probabilities, one must understand how a game is decomposed into these tokens.

Event-Level Containerization
An "Event" in the Polymarket schema is a high-level object that groups related markets. For the slug nba-atl-cha-2026-02-11, the event represents the matchup between the Atlanta Hawks and the Charlotte Hornets scheduled for February 11, 2026. The event slug is a unique, human-readable identifier typically extracted from the platform's URL. An event contains several metadata fields, including the startDate, endDate, and a markets array.   

Identifying the Moneyline Market
Within an NBA event, there are typically multiple markets. For a game like Hawks vs. Hornets, the markets might include:

Winner (Moneyline): A binary or multi-outcome market on which team will win the game.

Point Spread: A market on whether a team will win by a certain margin.

Total Points (Over/Under): A market on the combined score of both teams.

Halftime Winner: A market resolving based on the score at the end of the second quarter.

The "win probability" requested by analysts most often refers to the Moneyline market. In Polymarket's data model, this market maps to a set of clobTokenIds—unique 256-bit integers that identify the "Yes" and "No" tokens (or Team A and Team B tokens) on the exchange.   

Identifier	Description	Role in Implementation
Event Slug	nba-atl-cha-2026-02-11	Starting point for discovery.
Condition ID	0x... (Hex string)	Identifies the smart contract condition.
Token ID	0x... (Hex string)	Identifies the specific tradable asset (Asset ID).
Technical Implementation Workflow: From Slug to Time-Series
The process of using Python to retrieve win probability over time follows a linear four-step transformation of data. This workflow ensures that the researcher moves from a human-readable identifier to a precise, time-weighted dataset.

Step 1: Resolving the Event Slug via Gamma API
The initial request is a GET call to the Gamma API to retrieve the full event structure. The endpoint /events/slug/{slug} is optimized for this purpose. This call returns a JSON object that includes the markets array. Each market in this array contains its own slug, question, conditionId, and clobTokenIds.

A critical nuance in the NBA vertical is identifying which market in the array is the "full game" winner. This is generally the market where the question field matches the game's title without modifiers like "Spread" or "Total". For instance, the question for a Moneyline market might be "Will the Atlanta Hawks win their game against the Charlotte Hornets?".

Step 2: Mapping Teams to Token IDs
Once the Moneyline market is isolated, the developer must extract the clobTokenIds. In a binary market (Yes/No), the clobTokenIds array contains two strings. It is vital to correctly map these IDs to the respective teams. Typically, the first ID (Index 0) corresponds to the "Yes" outcome (e.g., Hawks win) and the second ID (Index 1) corresponds to the "No" outcome (e.g., Hornets win).   

In more complex NBA markets, such as those with "Negative Risk" (NegRisk), there may be multiple tokens representing different outcomes. However, for standard Moneyline bets, the two-token model is the standard.

Step 3: Querying the CLOB Price History
With the specific Token ID (also referred to as the Asset ID) obtained, the implementation shifts to the CLOB API. The endpoint GET /prices-history is the primary tool for temporal analysis. This endpoint requires the market parameter, which must be populated with the Token ID.   

To obtain a high-resolution view of win probability shifts (for instance, during the high-volatility period of an NBA game), the researcher should configure the fidelity and interval parameters. Fidelity defines the minutes between data points, while interval defines the total time window (e.g., max for all history).   

Step 4: Normalizing Price to Probability
The price (p) returned by the CLOB API is a float between 0 and 1. In prediction market theory, this price represents the market's implied probability of the outcome.

P(Win)=Price of Token
If the "Hawks" token is trading at 0.62, the market is pricing a 62% chance of a Hawks victory. The historical data allows the researcher to plot this value over time, revealing how the probability reacted to game-day news, starting lineup changes, or live scoring.

Python Implementation Strategies: SDK vs. REST
For professional-grade data extraction, developers have two primary paths: the official py-clob-client SDK or direct asynchronous REST requests.

Using the official Python SDK
The py-clob-client provides a high-level interface that simplifies interactions with the CLOB API. It abstracts the connection logic and provides typed methods for public data retrieval. To use the SDK for historical win probabilities, the developer initializes the ClobClient with the host https://clob.polymarket.com and the chain ID 137 (Polygon).   

While the SDK is excellent for trading (L2 methods), for simple data pulling (Public methods), many researchers prefer the flexibility of requests or httpx to handle the Gamma API, as the SDK is primarily focused on the CLOB.

Asynchronous Data Pipeline Construction
Given the rate limits imposed by Polymarket's infrastructure, an asynchronous approach using aiohttp is often superior for batch-pulling historical data for multiple NBA games. All API endpoints are protected by Cloudflare's throttling system, meaning that requests are queued or delayed rather than rejected outright. By managing concurrency, a Python script can efficiently traverse the Gamma discovery layer and the CLOB pricing layer without triggering significant latency penalties.   

Endpoint Group	10s Rate Limit	Usage Strategy
Gamma /events	500 requests	Batch discovery of game metadata.
Gamma /markets	300 requests	Extracting Token IDs for specific outcomes.
CLOB /prices-history	1,000 requests	Fetching high-fidelity probability series.
Data /trades	200 requests	Deep-dive into individual market moves.
   

Nuanced Insights: Market Dynamics and Data Quality
Extracting the data is only the first half of the task; interpreting it requires an understanding of the idiosyncrasies of prediction markets.

The Impact of Fees and Maker Rebates
On February 11, 2026, Polymarket updated its fee structure for sports markets. These fees affect the "taker" (the person removing liquidity) while providing rebates to "makers" (those placing limit orders). For the data analyst, this is relevant because taker fees can create a small divergence between the "true" probability and the "traded" price. However, since Polymarket uses midpoint pricing (the average of the best bid and ask) for its displayed probabilities, the impact of fees is often minimized in the historical price series.   

Fidelity and Interval Selection
The fidelity parameter is a critical lever in data extraction. A fidelity of 1 provides one-minute resolution, which is ideal for analyzing "live" market response to game events. For "previous games" that have already resolved, a researcher might choose a lower fidelity (e.g., 60 for hourly increments) to reduce the data payload while still capturing pre-game sentiment shifts.   

Resolution Logic for Postponed NBA Games
NBA markets have specific resolution criteria that can affect historical data points. If a game is postponed, the market remains open until the game is completed. In the event of a total cancellation without a make-up date, the market resolves 50-50. This can lead to a flat-line at 0.50 in the price history, which an automated model should flag as a non-predictive event.

Advanced Analytics: Beyond the Basic Price History
For more sophisticated win probability models, researchers often integrate secondary data sources and real-time monitoring tools.

The Goldsky Subgraph and Raw Trade Data
While the CLOB API provides price history, it does not always provide the full "tape" of every individual fill. Tools like poly_data utilize the Goldsky subgraph to collect "order-filled" events directly from the blockchain. This allows a researcher to see the exact maker/taker addresses, transaction hashes, and the size of each trade. This is particularly useful for identifying if a win probability shift was driven by a single large actor or a broad set of smaller participants.   

Cross-Platform Arbitrage and Signal Detection
NBA probabilities on Polymarket are often compared against traditional sportsbooks and other prediction markets like Kalshi. Third-party APIs such as balldontlie provide endpoints (GET /nba/v2/odds) that return odds from both prediction markets and sportsbooks simultaneously. A divergence between a sportsbook's American odds (e.g., -150) and Polymarket's implied probability (e.g., 65%) can serve as a signal for market inefficiency or impending movement.   

Real-Time Monitoring with WebSockets
For live games, relying on REST polling for win probability is inefficient. Polymarket's WebSocket service provides real-time updates for order books and trades. A Python script using websockets can subscribe to a specific Token ID and receive a stream of price updates with millisecond latency, allowing for the construction of a live win probability tracker.

Quantifying Market Depth and Liquidity
Win probability is only as reliable as the liquidity backing it. Polymarket provides endpoints to calculate the "estimated market price" for a given order size. Using the calculate_market_price method in the SDK, a researcher can determine how much capital it would take to move the win probability by 1%. This metric, often called "market depth," is essential for weighing the significance of price movements.   

Liquidity Metric	API Method	Description
Midpoint	get_midpoint	Average of best bid and ask (standard probability).
Spread	get_spread	Difference between best bid and ask (cost of entry).
Estimated Price	calculate_market_price	The price impact of a large trade (slippage).
Order Book	get_order_book	Full depth of bids and asks for a token.
Handling Authentication and Security
While fetching historical price data is a public operation, understanding the authentication layer is necessary for comprehensive pipeline development. Polymarket uses a two-tier authentication system:

L1 Authentication: Uses a private key to derive API credentials (API Key, Secret, Passphrase).

L2 Authentication: Uses the derived credentials to sign HMAC headers for authenticated requests.

For the developer only seeking to "pull data," these steps are optional. However, if the goal is to attribute the data requests to a "Builder" account—which can provide performance tracking and potentially higher rate limits—the developer must generate these keys via the Polymarket settings page.   

Future Outlook: AI Agents and Programmatic Sports Markets
The landscape of NBA data on Polymarket is shifting toward automated, agent-driven price discovery. Frameworks like Polymarket Agents allow for the deployment of AI bots that ingest news feeds and news articles to execute trades. This means that the historical win probability data pulled via Python is increasingly a reflection of automated logic and algorithmic sentiment analysis. For the data scientist, this emphasizes the need for high-frequency data collection (low fidelity) to capture the micro-adjustments made by these automated agents.   

Conclusions and Summary of Technical Steps
To obtain NBA win probability over time using Python, the researcher must implement a systematic approach that respects the platform's architectural boundaries. By starting with the game slug, the developer navigates through the Gamma API to resolve metadata and the CLOB API to retrieve time-series data.

The sequence of operations is summarized as follows:

Metadata Acquisition: Call https://gamma-api.polymarket.com/events/slug/{slug} to retrieve the event and its nested markets.

Market Selection: Identify the Moneyline market and extract the clobTokenIds for the desired team (usually index 0 for "Yes").

Historical Extraction: Use the GET /prices-history endpoint at https://clob.polymarket.com with the Token ID as the market parameter.   

Data Processing: Clean the resulting JSON to convert Unix timestamps to UTC and map the float price to a percentage-based win probability.

This methodology provides a robust foundation for building sports analytics tools, allowing for the observation of how market sentiment for NBA games fluctuates in real-time and across historical matchups. The transparency of the CLOB system, combined with the accessibility of its REST and WebSocket interfaces, positions Polymarket as a premier source for high-frequency sports probability data.

