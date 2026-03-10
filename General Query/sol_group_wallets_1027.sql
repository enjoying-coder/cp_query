WITH
constants AS (
    SELECT
        TIMESTAMP '2025-09-29' AS startDate,
        3 AS atLeastCount,
        ARRAY['1hhLTMgTAosUXSPwxedDv1gjKU9vDp9nMajjW6Upump', '1eDe5wnfzSRegB6Mg8MkVia8aCJrLAuhAztK7Yhpump', '1qT4WTUZPjbjiueSkoZFojDXdXaQVYrYm3YVh8apump'] AS group_tokens
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
            block_time >= (SELECT startDate FROM constants)
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
    WHERE evt_block_time >= (SELECT startDate FROM constants)
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
    WHERE evt_block_time >= (SELECT startDate FROM constants)
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
    WHERE call_block_time >= (SELECT startDate FROM constants)
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
    -- WHERE (SELECT include_meteora FROM constants)
),
trades_by_wallet AS (
    SELECT 
        wallet,
        COUNT(DISTINCT token) AS token_count
    FROM trades
    GROUP BY wallet
),
group_tokens_trades AS (
    SELECT
        *
    FROM 
        trades
    CROSS JOIN constants c
    WHERE 
        CONTAINS(c.group_tokens, token)
),
group_wallets AS (
    SELECT 
        wallet
    FROM group_tokens_trades
    GROUP BY wallet
    HAVING COUNT(DISTINCT token) >= (SELECT atLeastCount FROM constants)
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
        FROM group_tokens_trades
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
trades_for_ath AS (
    SELECT token, mc, block_time, block_slot
    FROM group_tokens_trades
    WHERE swap_type = 'buy' AND amount_usd > 100 AND amount_usd < 600
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
    WHERE evt_block_time >= TIMESTAMP '2025-07-01'
        
    UNION ALL

    SELECT 
        account_base_mint AS token,
        call_block_time AS mig_block_time,
        call_block_slot AS mig_block_slot
    FROM raydium_solana.raydium_launchpad_call_migrate_to_cpswap
    WHERE call_block_time >= TIMESTAMP '2025-07-01'

    UNION ALL

    SELECT
        mt.token,
        mt.block_time AS mig_block_time,
        mt.block_slot AS mig_block_slot
    FROM meteora_migrated_tokens mt
),
creators AS (
    SELECT 
        mint AS token,
        evt_block_time AS block_time,
        evt_tx_signer AS dev
    FROM pumpdotfun_solana.pump_evt_createevent
    WHERE evt_block_time >= TIMESTAMP '2025-07-01'

    UNION ALL

    SELECT
        call_account_arguments[7] AS token,
        call_block_time AS block_time,
        call_tx_signer AS dev
    FROM raydium_solana.raydium_launchpad_call_initialize
    WHERE call_block_time >= TIMESTAMP '2025-07-01'

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
        FROM group_tokens_trades
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
        FROM group_tokens_trades t
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
                WHEN cum_sell_token >= cum_buy_token * 0.95 AND cum_buy_token > 0 THEN
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
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) AS total_buy_token,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) AS total_sell_token,
            MIN(CASE WHEN t.swap_type = 'sell' THEN t.block_time END) AS min_sell_time,
            MIN(CASE WHEN t.swap_type = 'buy' THEN t.block_time END) AS min_buy_time,
            MIN(t.block_time) AS firstTradeTime,
            MIN(t.block_slot) AS firstTradeSlot
        FROM group_tokens_trades t
        GROUP BY t.wallet, t.token
    )
    SELECT
        a.*,
        fvs.valid_buy_sol,
        fvs.valid_sell_sol,
        fvs.valid_sell_time,
        CASE 
            WHEN fvs.valid_sell_time IS NULL THEN a.firstTradeTime + interval '30' minute
            ELSE LEAST(fvs.valid_sell_time, a.firstTradeTime + interval '30' minute)
        END AS valid_calc_time,
        CASE 
            WHEN fvs.valid_sell_time IS NULL THEN 2000
            ELSE DATE_DIFF('second', a.firstTradeTime, fvs.valid_sell_time)
        END AS hold_time
    FROM agg a
    LEFT JOIN first_valid_sell fvs
        ON a.wallet = fvs.wallet AND a.token = fvs.token
),
initialTradeTokens_temp AS (
    SELECT
        ta.wallet,
        ta.token,
        ta.firstTradeTime,
        ta.valid_calc_time,
        ta.valid_sell_time,
        ta.valid_buy_sol,
        ta.valid_sell_sol,
        CASE WHEN ta.valid_buy_sol > 0 THEN ta.valid_sell_sol / ta.valid_buy_sol ELSE 0 END AS hold_pnl,
        fb.buy_mc AS first_buy_mc,
        fb.buy_sol AS first_buy_sol,
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
valid_rise AS (
    SELECT
        itt.token,
        itt.wallet,
        MAX(t.mc) AS max_mc
    FROM initialTradeTokens itt
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
        COALESCE(DATE_DIFF('second', itt.firstTradeTime, itt.valid_sell_time), -1) AS valid_sell_duration,
        vr.max_mc AS valid_ath,
        vr.max_mc / itt.first_buy_mc AS valid_rise
    FROM initialTradeTokens itt
    LEFT JOIN valid_rise vr ON itt.token = vr.token AND itt.wallet = vr.wallet
),
walletInfo AS (
    SELECT
        iwt.wallet,
        tw.token_count,
        AVG(iwt.first_buy_mc) AS avg_buy_mc,
        AVG(iwt.valid_rise)AS avg_hold_rise,
        AVG(iwt.hold_pnl) AS avg_hold_pnl
    FROM initialWithRiseDuration iwt
    INNER JOIN group_wallets gw ON iwt.wallet = gw.wallet
    INNER JOIN trades_by_wallet tw ON iwt.wallet = tw.wallet
    GROUP BY iwt.wallet, tw.token_count
    HAVING tw.token_count < 20
)
SELECT * FROM walletInfo
ORDER BY avg_buy_mc ASC