WITH
constants AS (
    SELECT
        TIMESTAMP '2025-12-04' AS startDate,
        0 AS min_buy_mc,
        30 AS max_buy_mc,
        0 AS min_token_age,
        9000000 AS max_token_age,
        0 AS min_first_buy,
        100 AS max_first_buy,
        0 AS min_total_buy,
        100 AS max_total_buy,
        3 AS rise_filter,
        600 AS rise_filter_time,
        20 AS dump_percent,
        true AS include_pump,
        true AS include_launchlab,
        true AS include_meteora,
        1 AS bonding_selection_mode -- 1: only bonding buy, 2: only after-bonding buy, 3: both
),
meteora_bonding_configs AS (
    SELECT
        config,
        token_decimal AS decimal,
        pre_migration_token_supply / power(10, token_decimal) AS pre_supply,
        post_migration_token_supply / power(10, token_decimal) AS post_supply,
        quote_mint
    FROM meteora_solana.dynamic_bonding_curve_evt_evtcreateconfig
    WHERE evt_block_time >= TIMESTAMP '2025-08-20'
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
    WHERE evt_block_time >= TIMESTAMP '2025-08-20'
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
    WHERE call_block_time >= TIMESTAMP '2025-08-20'
),
trades AS (
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
            WHEN project = 'meteora' AND version = 3 THEN 'meteora-mig'
            WHEN project = 'meteora' AND version = 4 THEN 'meteoradbc'
        END AS project,
        tx_index
    FROM
        dex_solana.trades t
    WHERE
        block_time >= (SELECT startDate FROM constants)
        AND token_bought_mint_address != 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        AND token_sold_mint_address != 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        AND (t.project = 'pumpdotfun' OR t.project = 'pumpswap' OR t.project = 'raydium_launchlab' OR t.project = 'raydium' OR t.project = 'meteora' AND t.version = 3 OR t.project = 'meteora' AND t.version = 4)
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
    WHERE swap_type = 'buy' AND amount_usd > 30 AND amount_usd < 500
    GROUP BY token, block_slot
),
trades_for_ath AS (
    SELECT * FROM trades_for_ath_temp  WHERE rn % 15 = 1
),
trades_by_slot_base AS (
    SELECT
        token,
        block_slot,
        FIRST_VALUE(mc) OVER w AS open_mc,
        LAST_VALUE(mc) OVER w AS close_mc
    FROM (
        SELECT
            token,
            block_slot,
            mc,
            tx_index,
            ROW_NUMBER() OVER (PARTITION BY token, block_slot ORDER BY tx_index ASC) AS rn_asc,
            ROW_NUMBER() OVER (PARTITION BY token, block_slot ORDER BY tx_index DESC) AS rn_desc
        FROM trades
    ) t
    WHERE rn_asc = 1 OR rn_desc = 1
    WINDOW w AS (PARTITION BY token, block_slot ORDER BY tx_index ASC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
trades_by_slot AS (
    SELECT
        tbs.token,
        tbs.block_slot,
        tbs.open_mc,
        tbs.close_mc,
        LEAD(tbs.open_mc) OVER (PARTITION BY tbs.token ORDER BY tbs.block_slot) AS next_open_mc,
        LEAD(tbs.close_mc) OVER (PARTITION BY tbs.token ORDER BY tbs.block_slot) AS next_close_mc
    FROM (
        SELECT DISTINCT token, block_slot, open_mc, close_mc
        FROM trades_by_slot_base
    ) tbs
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
    WHERE evt_block_time >= TIMESTAMP '2025-08-20'
        
    UNION ALL

    SELECT 
        account_base_mint AS token,
        call_block_time AS mig_block_time,
        call_block_slot AS mig_block_slot
    FROM raydium_solana.raydium_launchpad_call_migrate_to_cpswap
    WHERE call_block_time >= TIMESTAMP '2025-08-20'

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
    FROM dune.rnadys410_team_4024.result_creators

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
    WITH ordered_trades AS (
        SELECT
            t.wallet,
            t.token,
            t.swap_type,
            t.token_amount,
            t.sol_amount,
            t.block_time,
            t.block_slot,
            -- Cumulative sum of buy and sell token_amounts, ordered by block_slot
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_token,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_token,
            -- Cumulative sum of buy and sell sol_amounts, ordered by block_slot
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.sol_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_sol,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.sol_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_sol
        FROM trades t
    ),
    valid_sell_points AS (
        SELECT
            wallet,
            token,
            block_time AS valid_sell_time,
            cum_buy_sol AS valid_buy_sol,
            cum_sell_sol AS valid_sell_sol,
            block_slot,
            ROW_NUMBER() OVER (PARTITION BY wallet, token ORDER BY block_slot ASC) AS rn,
            -- Find the first point where cumulative buy token = cumulative sell token
            CASE 
                WHEN cum_sell_token >= cum_buy_token * 0.95 AND cum_buy_token > 0 AND cum_sell_token < cum_buy_token * 1.05THEN
                    ROW_NUMBER() OVER (
                        PARTITION BY wallet, token
                        ORDER BY block_slot ASC
                    )
                ELSE NULL
            END AS match_rn
        FROM ordered_trades
    ),
    ranked_valid_sell AS (
        SELECT
            wallet,
            token,
            valid_sell_time,
            valid_buy_sol,
            valid_sell_sol,
            ROW_NUMBER() OVER (
                PARTITION BY wallet, token
                ORDER BY valid_sell_time
            ) AS rn
        FROM valid_sell_points
        WHERE match_rn > 0
    ),
    first_valid_sell AS (
        SELECT
            wallet,
            token,
            valid_sell_time,
            valid_buy_sol,
            valid_sell_sol
        FROM ranked_valid_sell
        WHERE rn = 1
    ),
    agg AS (
        SELECT
            t.wallet,
            t.token,
            SUM(CASE WHEN t.swap_type = 'buy' THEN -t.sol_amount ELSE t.sol_amount END) AS tokenProfit,
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.sol_amount ELSE 0 END) AS total_buy_sol,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.sol_amount ELSE 0 END) AS total_sell_sol,
            COUNT_IF(t.swap_type = 'buy') AS buy_count,
            COUNT_IF(t.swap_type = 'sell') AS sell_count,
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) AS total_buy_token,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) AS total_sell_token,
            MIN(CASE WHEN t.swap_type = 'sell' THEN t.block_time END) AS min_sell_time,
            MIN(CASE WHEN t.swap_type = 'buy' THEN t.block_time END) AS min_buy_time,
            MIN(t.block_time) AS firstTradeTime,
            MIN(t.block_slot) AS firstTradeSlot
        FROM trades t
        GROUP BY t.wallet, t.token
    )
    SELECT
        a.*,
        fvs.valid_buy_sol,
        fvs.valid_sell_sol,
        fvs.valid_sell_time,
        CASE 
            WHEN fvs.valid_sell_time IS NULL THEN a.firstTradeTime + interval '20' minute
            ELSE LEAST(fvs.valid_sell_time, a.firstTradeTime + interval '20' minute)
        END AS valid_calc_time,
        DATE_DIFF('second', a.firstTradeTime, fvs.valid_sell_time) AS hold_time
    FROM agg a
    LEFT JOIN first_valid_sell fvs
        ON a.wallet = fvs.wallet AND a.token = fvs.token
),
initialTradeTokens_temp AS (
    SELECT
        ta.wallet,
        ta.token,
        ta.firstTradeTime,
        ta.tokenProfit,
        ta.total_buy_sol,
        ta.total_sell_sol,
        ta.buy_count,
        ta.sell_count,
        ta.total_buy_token,
        ta.total_sell_token,
        ta.valid_calc_time,
        ta.valid_sell_time,
        ta.valid_buy_sol,
        ta.valid_sell_sol,
        (ta.min_sell_time - ta.min_buy_time) AS deltaTimeForBS,
        CASE WHEN ta.total_buy_sol > 0 THEN ta.total_sell_sol / ta.total_buy_sol ELSE 0 END AS pnl,
        CASE WHEN ta.valid_buy_sol > 0 THEN ta.valid_sell_sol / ta.valid_buy_sol ELSE 0 END AS hold_pnl,
        ta.hold_time,
        CASE WHEN tbs.next_open_mc > fb.buy_mc THEN tbs.next_open_mc / fb.buy_mc ELSE 1 END AS buy_rise,
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
        date_diff(
            'second',
            COALESCE(c.block_time, TIMESTAMP '2025-08-15'),
            fb.buy_block_time
        ) AS token_age,
        date_diff('second', fb.buy_block_time, m.mig_block_time) AS mig_time,
        CASE WHEN ta.wallet = c.dev THEN 1 ELSE 0 END AS is_creator,
        CASE WHEN fake.token IS NOT NULL THEN 1 ELSE 0 END AS is_fake,
        ROW_NUMBER() OVER (PARTITION BY ta.wallet, ta.token ORDER BY ta.firstTradeTime) AS rn
    FROM trade_aggregates ta
    LEFT JOIN first_buy_mc fb ON ta.wallet = fb.wallet AND ta.token = fb.token
    LEFT JOIN tokens_ath tath ON ta.token = tath.token
    LEFT JOIN migrations m ON ta.token = m.token
    LEFT JOIN creators c ON ta.token = c.token
    LEFT JOIN dune.rnadys410_team_4024.result_fake_tokens fake ON ta.token = fake.token
    LEFT JOIN trades_by_slot tbs ON ta.token = tbs.token AND ta.firstTradeSlot = tbs.block_slot
),
initialTradeTokens AS (
    SELECT *
    FROM initialTradeTokens_temp
    WHERE rn = 1
),
first_rise AS (
    SELECT
        itt.token,
        itt.wallet,
        MIN(CASE WHEN t.mc >= 2 * itt.first_buy_mc THEN t.block_time END) AS first_2x_block_time,
        MIN(CASE WHEN t.mc >= 3 * itt.first_buy_mc THEN t.block_time END) AS first_3x_block_time,
        MIN(CASE WHEN t.mc >= 5 * itt.first_buy_mc THEN t.block_time END) AS first_5x_block_time,
        MIN(CASE WHEN t.mc <= itt.first_buy_mc * (1 - (SELECT dump_percent FROM constants) / 100) THEN t.block_time END) AS first_dump_block_time,
        MIN(CASE WHEN t.mc >= itt.first_buy_mc * (SELECT rise_filter FROM constants) THEN t.block_time END) AS first_rise_block_time
    FROM initialTradeTokens itt
    JOIN trades_for_ath t ON t.token = itt.token AND t.block_time > itt.firstTradeTime AND t.block_time < itt.firstTradeTime + INTERVAL '20' minute
    GROUP BY itt.token, itt.wallet
),
valid_rise AS (
    SELECT
        itt.token,
        itt.wallet,
        MAX(t.mc) AS max_mc
    FROM initialTradeTokens itt
    -- Use a lateral join to only scan trades_for_ath for relevant tokens and time windows
    JOIN LATERAL (
        SELECT mc
        FROM trades_for_ath t
        WHERE t.token = itt.token
          AND t.block_time > itt.firstTradeTime
          AND t.block_time < itt.valid_calc_time
    ) t ON TRUE
    GROUP BY itt.token, itt.wallet
),
initialWithRiseDuration AS (
    SELECT
        itt.*,
        COALESCE(DATE_DIFF('second', itt.firstTradeTime, fx.first_2x_block_time), -1) AS first_2x_duration,
        COALESCE(DATE_DIFF('second', itt.firstTradeTime, fx.first_3x_block_time), -1) AS first_3x_duration,
        COALESCE(DATE_DIFF('second', itt.firstTradeTime, fx.first_5x_block_time), -1) AS first_5x_duration,
        COALESCE(DATE_DIFF('second', itt.firstTradeTime, fx.first_rise_block_time), -1) AS first_rise_duration,
        COALESCE(DATE_DIFF('second', itt.firstTradeTime, fx.first_dump_block_time), -1) AS first_dump_duration,
        COALESCE(DATE_DIFF('second', itt.firstTradeTime, itt.valid_sell_time), -1) AS valid_sell_duration,
        vr.max_mc AS valid_ath,
        vr.max_mc / itt.first_buy_mc AS valid_rise
    FROM initialTradeTokens itt
    LEFT JOIN first_rise fx ON itt.token = fx.token AND itt.wallet = fx.wallet
    LEFT JOIN valid_rise vr ON itt.token = vr.token AND itt.wallet = vr.wallet
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
            total_buy_sol >= c.min_total_buy AND total_buy_sol <= c.max_total_buy
        ) AS is_real,
        (
            CASE 
                WHEN token_age < 600 THEN 100
                WHEN token_age < 900 THEN 90
                WHEN token_age < 1200 THEN 80
                WHEN token_age < 3600 THEN 70
                ELSE 60
            END
        ) AS age_metrics,
        (
            CASE
                WHEN first_buy_mc < 15 THEN 100
                WHEN first_buy_mc < 30 THEN 85
                ELSE 70
            END
        ) AS mc_metrics,
        (
            CASE
                -- WHEN hold_pnl > 5 THEN 130
                WHEN hold_pnl > 3.5 THEN 100
                WHEN hold_pnl > 2 THEN 80
                WHEN hold_pnl > 1.3 THEN 60
                WHEN hold_pnl > 0.8 THEN 50
                ELSE 40
            END
        ) AS hold_pnl_metrics,
        (
            CASE 
                -- WHEN valid_rise > 6 THEN 140
                WHEN valid_rise > 4 THEN 120
                WHEN valid_rise > 2 THEN 100
                WHEN valid_rise > 2 THEN 80
                ELSE 40
            END
        ) AS valid_rise_metrics,
        (
            CASE
                WHEN hold_time < 30 THEN 60
                WHEN hold_time < 60 THEN 80
                WHEN hold_time < 300 THEN 90
                WHEN hold_time < 1800 THEN 100
                WHEN hold_time < 3600 THEN 80
                ELSE 70
            END
        ) AS hold_time_metrics,
        (
            CASE 
                -- WHEN pnl > 3.5 THEN 100
                WHEN pnl > 2.5 THEN 90
                WHEN pnl > 1.5 THEN 80
                WHEN pnl > 0.8 THEN 70
                ELSE 60
            END
        ) AS pnl_metrics,
        (
            CASE
                WHEN max_rise >= 6 THEN 100
                WHEN max_rise >= 4 THEN 80
                WHEN max_rise >= 2 THEN 70
                ELSE 50
            END
        ) AS rise_metrics,
        (
            CASE
                WHEN is_buy_before_ath = 1 THEN 100
                ELSE 50
            END
        ) AS buy_before_ath_metrics,
        200 - buy_rise * 100 AS buy_rise_metrics,
        (
            CASE 
                WHEN valid_rise > 8 OR hold_pnl > 4.5 THEN 6
                WHEN valid_rise > 6 OR hold_pnl > 3.5 THEN 5
                WHEN valid_rise > 4 OR hold_pnl > 2.5 THEN 4
                WHEN valid_rise > 2 OR hold_pnl > 1.5 THEN 2
                WHEN valid_rise < 1.5 OR hold_pnl < 0.8 THEN -1
                ELSE 0 
            END
        ) AS dev_metrics
    FROM initialWithRiseDuration it
    CROSS JOIN constants c
    WHERE total_buy_token >= total_sell_token - 1
),
walletInfo AS (
    SELECT
        ft.wallet,
        MIN(firstTradeTime) AS first_trade,
        MAX(firstTradeTime) AS last_trade,
        SUM(CASE WHEN is_real THEN total_buy_sol ELSE 0 END) AS buy_sol,
        SUM(CASE WHEN is_real THEN total_sell_sol ELSE 0 END) AS sell_sol,
        SUM(CASE WHEN is_real THEN tokenProfit ELSE 0 END) AS totalProfit,
        SUM(CASE WHEN is_real THEN LN(pnl + 1) / LN(2) ELSE 0 END) / COUNT_IF(is_real) AS pnl_log,
        SUM(CASE WHEN is_real THEN LN(hold_pnl + 1) / LN(2) ELSE 0 END) / COUNT_IF(is_real) AS hold_pnl_log,
        SUM(CASE WHEN is_real THEN LN(ath_mc / first_buy_mc) / LN(2) ELSE 0 END) / COUNT_IF(is_real) AS rise_log,
        SUM(CASE WHEN is_real THEN LN(valid_rise) / LN(2) ELSE 0 END) / COUNT_IF(is_real) AS hold_rise_log,
        (SUM(CASE WHEN is_real THEN total_sell_sol ELSE 0 END) - SUM(CASE WHEN is_real THEN total_buy_sol ELSE 0 END)) 
            / NULLIF(SUM(CASE WHEN is_real THEN total_buy_sol ELSE 0 END), 0) AS totalpnl,
        COUNT_IF(is_valid) AS count,
        COUNT_IF(is_real) AS real_count,
        COUNT_IF(is_real AND (buy_count > 3 OR hold_time < 10 OR buy_rise > 1.4)) AS bad_count,
        COUNT_IF(is_real AND is_bundle = 1) AS bundle_count,
        COUNT_IF(is_real AND first_rise_duration >= 0) AS rise_rise,
        COUNT_IF(is_real AND first_rise_duration >= 0 AND first_rise_duration <= (SELECT rise_filter_time FROM constants)) AS rise_rise_filter,
        COUNT_IF(is_real AND first_dump_duration < valid_sell_duration) AS dump_count,
        COUNT_IF(is_real AND first_2x_duration >= 0) AS rise_2x,
        COUNT_IF(is_real AND first_3x_duration >= 0) AS rise_3x,
        COUNT_IF(is_real AND first_5x_duration >= 0) AS rise_5x,
        COUNT_IF(is_real AND (first_3x_duration > 0 AND first_3x_duration < 600)) AS good_count,
        COUNT_IF(is_real AND first_5x_duration > 0 AND first_dump_duration = -1 OR (first_5x_duration > 0 AND first_5x_duration < first_dump_duration AND first_5x_duration < hold_time)) AS safe_count,
        SUM(CASE WHEN is_real THEN valid_rise ELSE 0 END) / COUNT_IF(is_real) AS hold_rise_avg,
        COUNT_IF(is_real AND valid_rise >= 2) AS hold_rise_2x,
        COUNT_IF(is_real AND valid_rise >= 3) AS hold_rise_3x,
        COUNT_IF(is_real AND valid_rise >= 5) AS hold_rise_5x,
        SUM(CASE WHEN is_real AND first_2x_duration >= 0 THEN first_2x_duration ELSE 0 END) AS rise_2x_time,
        SUM(CASE WHEN is_real AND first_3x_duration >= 0 THEN first_3x_duration ELSE 0 END) AS rise_3x_time,
        SUM(CASE WHEN is_real AND first_5x_duration >= 0 THEN first_5x_duration ELSE 0 END) AS rise_5x_time,
        SUM(CASE WHEN is_real AND first_rise_duration >= 0 THEN first_rise_duration ELSE 0 END) AS rise_rise_time,
        COUNT_IF(is_real AND valid_rise >= 4 AND hold_pnl <= 1.5 AND is_buy_before_ath = 1) AS fake_seller,
        COUNT_IF(is_real AND pnl >= 0.8) AS pnl_1x,
        COUNT_IF(is_real AND pnl >= 2) AS pnl_2x,
        COUNT_IF(is_real AND pnl >= 3) AS pnl_3x,
        COUNT_IF(is_real AND is_mig = 1) AS mig_count,
        SUM(CASE WHEN is_real AND is_mig = 1 THEN mig_time ELSE 0 END) AS mig_time,
        COUNT_IF(is_real AND is_buy_before_ath = 1) AS buy_before_ath_count,
        COUNT_IF(is_real AND is_sell_after_mig = 1) AS sell_after_mig_count,
        COUNT(DISTINCT ft.token) AS token_count,
        COUNT_IF(first_buy_project = 'pump' OR first_buy_project = 'launchlab' OR first_buy_project = 'meteoradbc') AS bonding_count,
        COUNT_IF(first_buy_project = 'meteoradbc' OR first_buy_project = 'meteora-mig') AS meteora_count,
        COUNT_IF(first_buy_project = 'pump' OR first_buy_project = 'pump-mig') AS pump_count,
        COUNT_IF(first_buy_project = 'launchlab' OR first_buy_project = 'launchlab-mig') AS launchlab_count,
        COUNT_IF(is_real AND (first_buy_project = 'meteoradbc' OR first_buy_project = 'meteora-mig')) AS meteora_valid_count,
        COUNT_IF(is_real AND (first_buy_project = 'pump' OR first_buy_project = 'pump-mig')) AS pump_valid_count,
        COUNT_IF(is_real AND (first_buy_project = 'launchlab' OR first_buy_project = 'launchlab-mig')) AS launchlab_valid_count,
        COUNT_IF(is_real AND is_creator = 1) AS creator_count,
        COUNT_IF(is_real AND buy_rise > 1.3) AS follow_count,
        SUM(CASE WHEN is_real THEN (age_metrics + mc_metrics * 2 + hold_pnl_metrics * 2 + valid_rise_metrics * 2 + hold_time_metrics + pnl_metrics + rise_metrics / 2 + buy_before_ath_metrics * 2 + buy_rise_metrics * 2) / 14.5 ELSE 0 END) AS total_metrics,
        SUM(CASE WHEN is_real AND dev_metrics > 0 THEN dev_metrics ELSE 0 END) - 
            (2 * pow(2, COUNT_IF(is_real AND dev_metrics < 0)) -1) AS devs_metrics,
        COUNT_IF(is_real AND is_fake = 1) AS fake_count,
        CASE 
            WHEN SUM(CASE WHEN is_real THEN 1.0 / NULLIF(valid_rise,0) ELSE 0 END) > 0
            THEN COUNT_IF(is_real) / SUM(CASE WHEN is_real THEN 1.0 / NULLIF(valid_rise,0) ELSE 0 END)
            ELSE NULL
        END AS harmonic_mean_calc_pnl
    FROM filteredTokens ft
    GROUP BY ft.wallet
),
results AS (
    SELECT
        wi.wallet,
        -- COALESCE(sw.start_again, ft.first_trade_solana) AS start_again,
        -- COALESCE(sw.silence, 0) AS silence_days,
        token_count,
        bonding_count,
        meteora_count,
        pump_count,
        launchlab_count,
        count,
        fake_count AS scam,
        follow_count,
        dump_count,
        count - real_count AS fake_history_count,
        real_count,
        good_count,
        safe_count,
        bad_count,
        meteora_valid_count,
        pump_valid_count,
        launchlab_valid_count,
        mig_count,
        fake_seller,
        ft.first_trade_solana AS first_trade,
        last_trade,
        CASE WHEN real_count > 0 THEN mig_time / real_count ELSE 0 END AS avg_mig_time,
        hold_rise_avg,
        CASE WHEN real_count > 0 THEN hold_rise_2x * 100.0 / real_count ELSE 0 END AS hold_rise_2x,
        CASE WHEN real_count > 0 THEN hold_rise_3x * 100.0 / real_count ELSE 0 END AS hold_rise_3x,
        CASE WHEN real_count > 0 THEN hold_rise_5x * 100.0 / real_count ELSE 0 END AS hold_rise_5x,
        CASE WHEN real_count > 0 THEN rise_rise * 100.0 / real_count ELSE 0 END AS rise_cond,
        CASE WHEN real_count > 0 THEN rise_rise_filter * 100.0 / real_count ELSE 0 END AS rise_cond_met,
        CASE WHEN real_count > 0 THEN rise_2x * 100.0 / real_count ELSE 0 END AS rise_2x,
        CASE WHEN rise_2x > 0 THEN rise_2x_time / rise_2x ELSE 0 END AS r_2x_time,
        CASE WHEN real_count > 0 THEN rise_3x * 100.0 / real_count ELSE 0 END AS rise_3x,
        CASE WHEN rise_3x > 0 THEN rise_3x_time / rise_3x ELSE 0 END AS r_3x_time,
        CASE WHEN real_count > 0 THEN rise_5x * 100.0 / real_count ELSE 0 END AS rise_5x,
        CASE WHEN rise_5x > 0 THEN rise_5x_time / rise_5x ELSE 0 END AS r_5x_time,
        CASE WHEN real_count > 0 THEN pnl_2x * 100.0 / real_count ELSE 0 END AS pnl_2x,   
        CASE WHEN real_count > 0 THEN pnl_3x * 100.0/ real_count ELSE 0 END AS pnl_3x,
        CASE WHEN real_count > 0 THEN bundle_count * 100.0 / real_count ELSE 0 END AS bundle_ratio,
        CASE WHEN real_count > 0 THEN mig_count * 100.0 / real_count ELSE 0 END AS mig_ratio,
        CASE WHEN real_count > 0 THEN buy_before_ath_count * 100.0 / real_count ELSE 0 END AS buy_before_ath,
        CASE WHEN real_count > 0 THEN sell_after_mig_count * 100.0 / real_count ELSE 0 END AS sell_after_mig,
        CASE WHEN real_count > 0 THEN pnl_1x * 100.0 / real_count ELSE 0 END AS win_rate,
        totalProfit,
        buy_sol,
        sell_sol,
        totalpnl,
        pnl_log,
        rise_log,
        hold_rise_log,
        hold_pnl_log,
        creator_count,
        CASE WHEN real_count > 0 THEN creator_count * 100.0 / real_count ELSE 0 END AS creator_ratio,
        CASE WHEN real_count > 0 THEN total_metrics / real_count ELSE 0 END AS total_metrics,
        CASE WHEN real_count > 0 THEN devs_metrics / real_count ELSE 0 END AS dev_metrics,
        CASE WHEN token_count > 0 THEN fake_count * 100.0 / token_count ELSE 0 END AS scam_ratio,
        harmonic_mean_calc_pnl
    FROM walletInfo wi
    LEFT JOIN LATERAL (
        SELECT MIN(block_time) AS first_trade_solana
        FROM dex_solana.trades
        WHERE block_time >= TIMESTAMP '2025-08-08' AND trader_id = wi.wallet
    ) ft ON TRUE
    -- LEFT JOIN dune.rnadys410_team_4024.result_silence_wallets sw ON sw.wallet = wi.wallet
)
SELECT
    r.*,
    ROW_NUMBER() OVER (ORDER BY r.total_metrics DESC) AS row_num
FROM results r
WHERE 
    r.count >= 2
    AND r.creator_count <= 1
    AND r.token_count <= 40
    AND r.scam <= 1
    AND r.rise_2x >= 30
    AND r.rise_3x > 0
    AND r.last_trade - r.first_trade >= INTERVAL '12' HOUR
    AND (r.pump_count > 0 OR r.launchlab_count > 0)
    AND r.win_rate >= 30
    -- AND r.last_trade >= TIMESTAMP '2025-10-11'
    
    -- AND r.first_trade >= TIMESTAMP '2025-09-20'
ORDER BY r.total_metrics DESC