WITH
constants AS (
    SELECT
        TIMESTAMP '2025-08-30' AS startDate,
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
        ARRAY[] AS blacklist_tokens,
        ARRAY[] AS whitelist_tokens,
        true AS include_pump,
        true AS include_launchlab,
        true AS include_meteora,
        1 AS bonding_selection_mode -- 1: only bonding buy, 2: only after-bonding buy, 3: both
),
token_symbol AS (
    SELECT
        token,
        MIN(symbol) AS symbol
    FROM (
        SELECT DISTINCT
            CASE
                WHEN token_bought_mint_address IN ('So11111111111111111111111111111111111111112',
                                                   'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
                    THEN token_sold_mint_address
                ELSE token_bought_mint_address
            END AS token,
            CASE
                WHEN token_bought_mint_address IN ('So11111111111111111111111111111111111111112',
                                                   'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
                    THEN token_sold_symbol
                ELSE token_bought_symbol
            END AS symbol
        FROM dex_solana.trades
        WHERE block_time >= (SELECT startDate FROM constants)
    ) t
    GROUP BY token
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
            block_time >= TIMESTAMP '2025-07-20'
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
    WHERE evt_block_time >= TIMESTAMP '2025-07-20'
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
    WHERE evt_block_time >= TIMESTAMP '2025-07-20'
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
    WHERE call_block_time >= TIMESTAMP '2025-07-20'
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
trades_for_ath_temp AS (
    SELECT 
        token, 
        MIN_BY(mc, tx_index) AS mc, 
        MIN_BY(block_time, tx_index) AS block_time, 
        MIN(tx_index) AS tx_index,
        block_slot,
        ROW_NUMBER() OVER (PARTITION BY token, block_slot ORDER BY MIN(tx_index) ASC) AS rn
    FROM trades
    WHERE swap_type = 'buy' AND amount_usd > 50 AND amount_usd < 300
    GROUP BY token, block_slot
),
trades_for_ath AS (
    SELECT * FROM trades_for_ath_temp WHERE rn % 3 = 1
),
tokens_ath AS (
    SELECT
        token,
        mc AS ath_mc,
        block_time AS ath_block_time,
        block_slot AS ath_block_slot
    FROM (
        SELECT
            token,
            mc,
            block_time,
            block_slot,
            ROW_NUMBER() OVER (PARTITION BY token ORDER BY mc DESC) AS rn
        FROM trades_for_ath
    ) t
    WHERE rn = 1
),
migrations AS (
    SELECT
        mint AS token,
        evt_block_time AS mig_block_time,
        evt_block_slot AS mig_block_slot
    FROM pumpdotfun_solana.pump_evt_completeevent
    WHERE evt_block_time >= TIMESTAMP '2025-07-15'
        
    UNION ALL

    SELECT 
        account_base_mint AS token,
        call_block_time AS mig_block_time,
        call_block_slot AS mig_block_slot
    FROM raydium_solana.raydium_launchpad_call_migrate_to_cpswap
    WHERE call_block_time >= TIMESTAMP '2025-07-15'

    UNION ALL

    SELECT
        mt.token,
        mt.block_time AS mig_block_time,
        mt.block_slot AS mig_block_slot
    FROM meteora_migrated_tokens mt
),
creators AS (
    SELECT 
        token,
        block_time,
        dev
    FROM dune.schoolelite564.result_creators

    UNION ALL

    SELECT
        mt.token,
        mt.block_time,
        mt.dev
    FROM meteora_bonding_tokens mt
),
first_buy_mc AS (
    SELECT
        wallet,
        token,
        mc AS buy_mc,
        sol_amount AS buy_sol,
        project,
        block_time AS buy_block_time,
        block_slot AS buy_block_slot
    FROM (
        SELECT
            wallet,
            token,
            mc,
            project,
            block_time,
            block_slot,
            sol_amount,
            ROW_NUMBER() OVER (PARTITION BY wallet, token ORDER BY block_time) AS rn
        FROM trades
        WHERE swap_type = 'buy'
    ) t
    WHERE rn = 1
), 
trade_aggregates AS (
    SELECT
        t.wallet,
        t.token,
        SUM(CASE WHEN t.swap_type = 'buy' THEN -t.sol_amount ELSE t.sol_amount END) AS tokenProfit,
        SUM(CASE WHEN t.swap_type = 'buy' THEN t.sol_amount ELSE 0 END) AS total_buy_sol,
        SUM(CASE WHEN t.swap_type = 'sell' THEN t.sol_amount ELSE 0 END) AS total_sell_sol,
        SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) AS total_buy_token,
        SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) AS total_sell_token,
        MIN(CASE WHEN t.swap_type = 'sell' THEN t.block_time END) AS min_sell_time,
        MIN(CASE WHEN t.swap_type = 'buy' THEN t.block_time END) AS min_buy_time,
        MIN(t.block_time) AS firstTradeTime,
        MIN(t.block_slot) AS firstTradeSlot
    FROM trades t
    GROUP BY t.wallet, t.token
),
initialTradeTokens_temp AS (
    SELECT
        ta.wallet,
        ta.token,
        ta.firstTradeTime,
        ta.tokenProfit,
        ta.total_buy_sol,
        ta.total_sell_sol,
        ta.total_buy_token,
        ta.total_sell_token,
        (ta.min_sell_time - ta.min_buy_time) AS deltaTimeForBS,
        CASE WHEN ta.total_buy_sol > 0 THEN ta.total_sell_sol / ta.total_buy_sol ELSE 0 END AS pnl,
        fb.buy_mc AS first_buy_mc,
        fb.project AS first_buy_project,
        fb.buy_sol AS first_buy_sol,
        COALESCE(tath.ath_mc, fb.buy_mc) AS ath_mc,
        COALESCE(tath.ath_mc, fb.buy_mc) / fb.buy_mc AS max_rise,
        CASE WHEN fb.buy_block_slot < COALESCE(tath.ath_block_slot, 0) THEN 1 ELSE 0 END AS is_buy_before_ath,
        CASE WHEN ta.min_sell_time > m.mig_block_time THEN 1 ELSE 0 END AS is_sell_after_mig,
        CASE WHEN fb.buy_block_slot < COALESCE(m.mig_block_slot, 0) THEN 1 ELSE 0 END AS is_mig,
        CASE WHEN fb.buy_block_slot >= COALESCE(m.mig_block_slot, 0) - 2 AND COALESCE(m.mig_block_slot, 0) > 0 THEN 1 ELSE 0 END AS is_bundle,
        fb.buy_block_slot,
        COALESCE(m.mig_block_slot, 0) AS mig_block_slot,
        date_diff('second', c.block_time, fb.buy_block_time) AS token_age,
        date_diff('second', fb.buy_block_time, m.mig_block_time) AS mig_time,
        CASE WHEN ta.wallet = c.dev THEN 1 ELSE 0 END AS is_creator,
        CASE WHEN fake.token IS NOT NULL THEN 1 ELSE 0 END AS is_fake,
        ROW_NUMBER() OVER (PARTITION BY ta.wallet, ta.token ORDER BY ta.firstTradeTime) AS rn
    FROM trade_aggregates ta
    LEFT JOIN first_buy_mc fb ON ta.wallet = fb.wallet AND ta.token = fb.token
    LEFT JOIN tokens_ath tath ON ta.token = tath.token
    LEFT JOIN migrations m ON ta.token = m.token
    LEFT JOIN creators c ON ta.token = c.token
    LEFT JOIN dune.schoolelite564.result_fake_tokens fake ON ta.token = fake.token
),
initialTradeTokens AS (
    SELECT *
    FROM initialTradeTokens_temp
    WHERE rn = 1
),
filteredTokens AS (
    SELECT
        it.*,
        (
            first_buy_mc >= c.min_buy_mc AND first_buy_mc <= c.max_buy_mc AND
            token_age >= c.min_token_age AND token_age <= c.max_token_age AND
            (
                (c.bonding_selection_mode = 1 AND first_buy_project IN ('pump', 'launchlab', 'meteoradbc')) OR 
                (c.bonding_selection_mode = 2 AND first_buy_project NOT IN ('pump', 'launchlab', 'meteoradbc')) OR
                c.bonding_selection_mode = 3
            ) AND
            (
                (c.include_pump AND first_buy_project IN ('pump', 'pump-mig')) OR
                (c.include_launchlab AND first_buy_project IN ('launchlab', 'launchlab-mig')) OR
                (c.include_meteora AND first_buy_project IN ('meteoradbc', 'meteora-mig'))
            ) AND
            first_buy_sol >= c.min_first_buy AND first_buy_sol <= c.max_first_buy AND
            total_buy_sol >= c.min_total_buy AND total_buy_sol <= c.max_total_buy
        ) AS is_valid,
        (
            first_buy_mc >= c.min_buy_mc AND first_buy_mc <= c.max_buy_mc AND
            token_age >= c.min_token_age AND token_age <= c.max_token_age AND
            (
                (c.bonding_selection_mode = 1 AND first_buy_project IN ('pump', 'launchlab', 'meteoradbc')) OR 
                (c.bonding_selection_mode = 2 AND first_buy_project NOT IN ('pump', 'launchlab', 'meteoradbc')) OR
                c.bonding_selection_mode = 3
            ) AND
            (
                (c.include_pump AND first_buy_project IN ('pump', 'pump-mig')) OR
                (c.include_launchlab AND first_buy_project IN ('launchlab', 'launchlab-mig')) OR
                (c.include_meteora AND first_buy_project IN ('meteoradbc', 'meteora-mig'))
            ) AND
            first_buy_sol >= c.min_first_buy AND first_buy_sol <= c.max_first_buy AND
            total_buy_sol >= c.min_total_buy AND total_buy_sol <= c.max_total_buy AND
            NOT (first_buy_sol <= 0.1 OR (token_age >= 1800 AND first_buy_mc <= 6))
        ) AS is_real
    FROM initialTradeTokens it
    CROSS JOIN constants c
    WHERE total_buy_token >= total_sell_token - 1
),
wallet_top_tokens AS (
    SELECT
        wallet,
        token,
        ath_mc,
        ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY ath_mc DESC) AS rn
    FROM filteredTokens
    WHERE is_real
),
wallet_patterns AS (
    SELECT
        wtt.wallet,
        array_join(array_agg(ts.symbol ORDER BY wtt.ath_mc DESC), ',') AS pattern
    FROM wallet_top_tokens wtt
    LEFT JOIN token_symbol ts ON wtt.token = ts.token
    WHERE wtt.rn <= 3
    GROUP BY wtt.wallet
),
balance_filtered AS (
    SELECT address
    FROM solana_utils.latest_balances
    WHERE sol_balance > 0.1 AND block_time >= TIMESTAMP '2025-08-20'
)
SELECT wp.wallet, wp.pattern, fr.* FROM dune.rnadys410_team_4024.result_heaven fr
INNER JOIN wallet_patterns wp ON fr.wallet = wp.wallet
INNER JOIN balance_filtered bf ON fr.wallet = bf.address
WHERE fr.bonding_count <= 6 AND fr.scam <= 1 AND fr.first_trade >= TIMESTAMP '2025-08-31' AND fr.token_count - fr.bonding_count < 5
ORDER BY fr.total_metrics DESC