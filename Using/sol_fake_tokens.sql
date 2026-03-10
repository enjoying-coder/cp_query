WITH
constants AS (
    SELECT
        TIMESTAMP '2025-12-01' AS startDate,
        0 AS min_buy_mc,
        30 AS max_buy_mc,
        0 AS min_token_age,
        60 AS max_token_age,
        0 AS min_first_buy,
        100 AS max_first_buy,
        0 AS min_total_buy,
        100 AS max_total_buy,
        3 AS rise_filter,
        120 AS rise_filter_time,
        40 AS dump_percent,
        ARRAY['6DEa18xxCgx2SgBTJbFdEY6PTNZMWvG2nDv47LHspump', '2VNFmf1tBqzAQyKC2WrcgUb6UKgVnNb9KvWrM3ndpump', 'CjuuJgZv6FAuD1KDHpVVAsvtFxmJCxxrGCy4jBNhpump', 'A8ZgV1h58pcyXKzPLWtWPb4crFgseqdycL37wLY5pump', 'r7LWUgjn8iDEvYJkeinJdXgdWgsmz4quwtNZgvpbonk', 'Cac8nAFnhiSKKiAyeUzf9SMYkrr1G3TTD717tNWYjups'] AS blacklist_tokens,
        ARRAY[] AS whitelist_tokens,
        true AS include_pump,
        true AS include_launchlab,
        true AS include_meteora,
        1 AS bonding_selection_mode -- 1: only bonding buy, 2: only after-bonding buy, 3: both
),
sol_price_table AS (
    SELECT 
        block_date,
        token_bought_amount / token_sold_amount AS price
    FROM (
        SELECT 
            block_date,
            token_bought_amount,
            token_sold_amount,
            ROW_NUMBER() OVER (PARTITION BY block_date ORDER BY block_time) as rn
        FROM dex_solana.trades 
        WHERE 
            block_time >= TIMESTAMP '2025-12-01'
            AND project = 'raydium'
            AND token_bought_mint_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
            AND project_program_id = '3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv'
            AND token_sold_amount > 1
    ) 
    WHERE rn = 1
),
latest_sol_price AS (
    SELECT price as latest_price
    FROM sol_price_table
    ORDER BY block_date DESC
    LIMIT 1
),
meteora_bonding_configs AS (
    SELECT
        config,
        token_decimal AS decimal,
        pre_migration_token_supply / power(10, token_decimal) AS pre_supply,
        post_migration_token_supply / power(10, token_decimal) AS post_supply,
        quote_mint
    FROM meteora_solana.dynamic_bonding_curve_evt_evtcreateconfig
    WHERE evt_block_time >= TIMESTAMP '2025-10-01'
      AND quote_mint IN ('So11111111111111111111111111111111111111112', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
),
meteora_bonding_tokens AS (
    SELECT
        pool,
        base_mint AS token,
        c.decimal,
        c.pre_supply,
        c.post_supply,
        CASE WHEN c.quote_mint='So11111111111111111111111111111111111111112' THEN 'sol' ELSE 'usdc' END AS quote_mint,
        evt_block_time AS block_time,
        evt_tx_signer AS dev
    FROM meteora_solana.dynamic_bonding_curve_evt_evtinitializepool
    INNER JOIN meteora_bonding_configs c ON c.config = meteora_solana.dynamic_bonding_curve_evt_evtinitializepool.config
    WHERE evt_block_time >= TIMESTAMP '2025-12-01'
),
meteora_migrated_tokens AS (
    SELECT 
        account_pool AS pool,
        account_base_mint AS token,
        CASE WHEN account_quote_mint='So11111111111111111111111111111111111111112' THEN 'sol' ELSE 'usdc' END AS quote_mint,
        t.decimal,
        t.post_supply,
        call_block_slot AS block_slot,
        call_block_time AS block_time
    FROM meteora_solana.dynamic_bonding_curve_call_migration_damm_v2 
    INNER JOIN meteora_bonding_tokens t ON t.token = meteora_solana.dynamic_bonding_curve_call_migration_damm_v2.account_base_mint
    WHERE call_block_time >= TIMESTAMP '2025-12-01'
),
meteora_all_pre_trades AS (
    SELECT
        t.pool,
        t.token,
        s.wallet,
        CASE WHEN s.trade_direction = 1 THEN 'buy' ELSE 'sell' END AS trade_type,
        CASE
            WHEN s.trade_direction = 1 THEN s.input_amount / power(10, CASE WHEN t.quote_mint = 'sol' THEN 9 ELSE 6 END)
            ELSE s.output_amount / power(10, CASE WHEN t.quote_mint = 'sol' THEN 9 ELSE 6 END)
        END AS base_amount,
        CASE
            WHEN s.trade_direction = 1 THEN s.output_amount / power(10, t.decimal)
            ELSE s.input_amount / power(10, t.decimal)
        END AS token_amount,
        'meteoradbc' AS project,
        s.block_time,
        s.block_slot,
        s.block_date,
        t.pre_supply AS supply,
        t.quote_mint,
        s.tx_index
    FROM (
        SELECT
            pool,
            evt_tx_signer AS wallet,
            evt_block_time AS block_time,
            evt_block_slot AS block_slot,
            evt_block_date AS block_date,
            cast(json_extract(swap_result, '$.SwapResult.actual_input_amount') AS double) AS input_amount,
            cast(json_extract(swap_result, '$.SwapResult.output_amount') AS double) AS output_amount,
            trade_direction,
            evt_tx_index AS tx_index
        FROM meteora_solana.dynamic_bonding_curve_evt_evtswap
        WHERE evt_block_time >= (SELECT startDate FROM constants)
    ) s
    LEFT JOIN meteora_bonding_tokens t ON s.pool = t.pool

    UNION ALL

    SELECT
        t.pool,
        t.token,
        s.wallet,
        CASE WHEN s.trade_direction = 1 THEN 'buy' ELSE 'sell' END AS trade_type,
        CASE
            WHEN s.trade_direction = 1 THEN s.input_amount / power(10, CASE WHEN t.quote_mint = 'sol' THEN 9 ELSE 6 END)
            ELSE s.output_amount / power(10, CASE WHEN t.quote_mint = 'sol' THEN 9 ELSE 6 END)
        END AS base_amount,
        CASE
            WHEN s.trade_direction = 1 THEN s.output_amount / power(10, t.decimal)
            ELSE s.input_amount / power(10, t.decimal)
        END AS token_amount,
        'meteora-mig' AS project,
        s.block_time,
        s.block_slot,
        s.block_date,
        t.post_supply AS supply,
        t.quote_mint,
        s.tx_index
    FROM (
        SELECT
            pool,
            evt_tx_signer AS wallet,
            evt_block_time AS block_time,
            evt_block_slot AS block_slot,
            evt_block_date AS block_date,
            actual_amount_in AS input_amount,
            cast(json_extract(swap_result, '$.SwapResult.output_amount') AS double) AS output_amount,
            trade_direction,
            evt_tx_index AS tx_index
        FROM meteora_solana.cp_amm_evt_evtswap
        WHERE evt_block_time >= (SELECT startDate FROM constants)
    ) s
    INNER JOIN meteora_migrated_tokens t ON s.pool = t.pool
),
pump_launchlab_trades AS (
    SELECT
        CASE
            WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN token_sold_mint_address
            ELSE token_bought_mint_address
        END AS token,
        trader_id AS wallet,
        CASE
            WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN 'sell'
            ELSE 'buy'
        END AS swap_type,
        CASE
            WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN token_bought_amount
            ELSE token_sold_amount
        END AS sol_amount,
        CASE
            WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN token_sold_amount
            ELSE token_bought_amount
        END AS token_amount,
        CAST(amount_usd * pow(10, 6) / (
            CASE
                WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN token_sold_amount
                ELSE token_bought_amount
            END
        ) AS DOUBLE) AS mc,
        block_time,
        block_slot,
        amount_usd,
        CASE
            WHEN project = 'pumpdotfun' THEN 'pump'
            WHEN project = 'pumpswap' THEN 'pump-mig'
            WHEN project = 'raydium_launchlab' THEN 'launchlab'
            WHEN project = 'raydium' THEN 'launchlab-mig'
        END AS project,
        tx_index
    FROM
        dex_solana.trades t
    WHERE
        block_time >= (SELECT startDate FROM constants)
        -- AND (
        --     ((SELECT include_pump FROM constants) AND (project = 'pumpdotfun' OR project = 'pumpswap'))
        --     OR
        --     ((SELECT include_launchlab FROM constants) AND (project = 'raydium_launchlab' OR project = 'raydium'))
        -- )
        AND token_bought_mint_address != 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        AND token_sold_mint_address != 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        AND (t.project = 'pumpdotfun' OR t.project = 'pumpswap' OR t.project = 'raydium_launchlab' OR t.project = 'raydium')
),
trades AS (
    SELECT 
        token,
        wallet,
        swap_type,
        sol_amount,
        token_amount,
        mc,
        project, 
        CAST(block_time AS TIMESTAMP) AS block_time,
        block_slot,
        amount_usd,
        tx_index
    FROM pump_launchlab_trades

    UNION ALL

    SELECT 
        mbt.token,
        mbt.wallet,
        mbt.trade_type AS swap_type,
        CASE 
            WHEN mbt.quote_mint = 'sol' THEN mbt.base_amount
            ELSE mbt.base_amount / COALESCE(spt.price, lsp.latest_price, 185)
        END AS sol_amount,
        mbt.token_amount, 
        CASE 
            WHEN mbt.quote_mint = 'sol' THEN mbt.base_amount * COALESCE(spt.price, lsp.latest_price, 185) * mbt.supply / mbt.token_amount / 1000
            ELSE mbt.base_amount * mbt.supply / mbt.token_amount / 1000
        END AS mc,
        mbt.project, 
        mbt.block_time,
        mbt.block_slot,
        CASE 
            WHEN mbt.quote_mint = 'sol' THEN mbt.base_amount * COALESCE(spt.price, lsp.latest_price, 185)
            ELSE mbt.base_amount
        END AS amount_usd,
        mbt.tx_index
    FROM meteora_all_pre_trades mbt
    LEFT JOIN sol_price_table spt
        ON spt.block_date = mbt.block_date
    CROSS JOIN latest_sol_price lsp
),
creators AS (
    SELECT 
        token,
        block_time,
        dev
    FROM dune.rnadys410_team_4024.result_creators

    UNION ALL

    SELECT
        mt.token,
        mt.block_time,
        mt.dev
    FROM meteora_bonding_tokens mt
),
dev_sell_count AS (
    SELECT
        trades.token,
        COUNT(CASE WHEN swap_type = 'sell' AND c.dev = trades.wallet THEN 1 END) AS dev_sell_count
    FROM trades
    INNER JOIN creators c ON trades.token = c.token
    GROUP BY trades.token
),
trades_by_slot AS (
    SELECT
        token,
        block_slot,
        MIN_BY(mc, tx_index) AS open_mc,
        MAX_BY(mc, tx_index) AS close_mc,
        project
    FROM trades
    GROUP BY token, block_slot, project
),
trades_by_slot_grouped AS (
SELECT
    token,
    project,
    block_slot,
    open_mc,
    close_mc,
    LEAD(close_mc, 2) OVER (PARTITION BY token, project ORDER BY block_slot) AS close_mc_grouped
FROM trades_by_slot
)
SELECT trades_by_slot_grouped.token
FROM 
    trades_by_slot_grouped 
LEFT JOIN dev_sell_count dsc ON trades_by_slot_grouped.token = dsc.token
WHERE 
    (project = 'pump' OR project = 'launchlab' OR project = 'meteoradbc') AND (close_mc - open_mc > 30 OR open_mc - close_mc > 25 OR (open_mc >= 25 AND close_mc_grouped <= 10) OR open_mc >= 18 AND close_mc <= 9)
    OR (project = 'pump-mig' OR project = 'launchlab-mig' OR project = 'meteora-mig') AND ((open_mc >= 50 AND open_mc >= close_mc_grouped * 2) OR (open_mc <= 50 AND open_mc - close_mc_grouped >= 30))
    OR dsc.dev_sell_count >= 10
GROUP BY trades_by_slot_grouped.token