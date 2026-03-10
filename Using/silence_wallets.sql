WITH filtered_trades AS (
  SELECT trader_id, block_date
  FROM dex_solana.trades
  WHERE block_date >= DATE '2025-09-30'
),
ordered_trades AS (
  SELECT
    trader_id AS wallet,
    block_date,
    LAG(block_date) OVER (PARTITION BY trader_id ORDER BY block_date) AS prev_date
  FROM filtered_trades
),
gaps AS (
  SELECT
    wallet,
    block_date AS start_again,
    prev_date,
    EXTRACT(DAY FROM block_date - prev_date) AS silence
  FROM ordered_trades
  WHERE prev_date IS NOT NULL
    AND EXTRACT(DAY FROM block_date - prev_date) > 3
),
max_gap AS (
  SELECT
    wallet,
    start_again,
    silence,
    ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY silence DESC) AS rn
  FROM gaps
),
first_trade AS (
  SELECT
    trader_id AS wallet,
    MIN(block_date) AS start_again
  FROM filtered_trades
  GROUP BY trader_id
),
results AS (
SELECT
  ft.wallet,
  COALESCE(mg.start_again, ft.start_again) AS start_again,
  COALESCE(mg.silence, 0) AS silence
FROM first_trade ft
LEFT JOIN max_gap mg
  ON ft.wallet = mg.wallet AND mg.rn = 1
)
SELECT * FROM results
WHERE
    start_again >= TIMESTAMP '2025-10-15'
    AND silence >= 5
