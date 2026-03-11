WITH 
launchlab_mints AS (
    SELECT
        MIN(block_time) AS block_time,
        MIN(block_slot) AS block_slot,
        account_arguments[7] AS token,
        tx_signer AS dev
    FROM solana.instruction_calls
    WHERE block_time >= NOW() - INTERVAL '30' day
      AND executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
        AND (
      varbinary_starts_with(data, 0xafaf6d1f0d989bed)
      OR varbinary_starts_with(data, 0x4399af27)
      )
      AND tx_success = true
    GROUP BY tx_id,
        CASE
            WHEN account_arguments[4] IN (
                'FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1',
                'BuM6KDpWiTcxvrpXywWFiw45R2RNH8WURdvqoTDV1BW4'
            )
            THEN 'LetsBonk'
            ELSE 'LaunchLabs'
        END,
        account_arguments[7],
        tx_signer
),
creators AS (
    SELECT 
        mint AS token,
        evt_block_time AS block_time,
        evt_block_slot AS block_slot,
        evt_tx_signer AS dev
    FROM pumpdotfun_solana.pump_evt_createevent
    WHERE evt_block_time >= NOW() - INTERVAL '30' day

    UNION ALL

    SELECT
        token,
        block_time,
        block_slot,
        dev
    FROM launchlab_mints
)

SELECT
  *
FROM creators