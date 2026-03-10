WITH
constants AS (
    SELECT
        TIMESTAMP '2025-10-01' AS startDate,
        1 AS atLeastCount,
        ARRAY[0x3e8e54ec4f33ac7f3e59a615d4a13090fd204444] AS group_tokens,
        2 AS aster_price,
        1320 AS bnb_price
),
migrations AS (
    SELECT 
        token,
        evt_block_time AS mig_block_time,
        evt_block_number AS mig_block_slot
    FROM 
        four_meme_multichain.tokenmanager2_evt_tradestop WHERE chain = 'bnb'
),
creators AS (
    SELECT 
        tc.token AS token,
        COALESCE(la.quote, 0x0000000000000000000000000000000000000000) AS quote,
        tc.evt_block_time AS block_time,
        tc.creator AS dev
    FROM 
        four_meme_multichain.tokenmanager2_evt_tokencreate tc
    LEFT JOIN four_meme_multichain.tokenmanager2_evt_liquidityadded la ON tc.token = la.base
    WHERE tc.chain = 'bnb'
),
trades AS (
    SELECT 
        fmt.token,
        fmt.evt_tx_from AS wallet,
        'buy' AS swap_type,
        CASE
            WHEN c.quote = 0x0000000000000000000000000000000000000000 THEN fmt.cost / 1e18
            WHEN c.quote = 0x000Ae314E2A2172a039B26378814C252734f556A THEN fmt.cost * (SELECT aster_price FROM constants) / (SELECT bnb_price FROM constants) / 1e18
            WHEN c.quote = 0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d THEN fmt.cost / (SELECT bnb_price FROM constants) / 1e18
            WHEN c.quote = 0x55d398326f99059ff775485246999027b3197955 THEN fmt.cost / (SELECT bnb_price FROM constants) / 1e18
        END AS bnb_amount,
        fmt.amount / 1e18 AS token_amount,
        CASE
            WHEN c.quote = 0x0000000000000000000000000000000000000000 THEN fmt.price * (SELECT bnb_price FROM constants) / 1e12
            WHEN c.quote = 0x000Ae314E2A2172a039B26378814C252734f556A THEN fmt.price / (SELECT aster_price FROM constants) / 1e12
            WHEN c.quote = 0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d THEN fmt.price / 1e12
            WHEN c.quote = 0x55d398326f99059ff775485246999027b3197955 THEN fmt.price / 1e12
        END AS mc,
        fmt.evt_block_time AS block_time,
        fmt.evt_block_number AS block_slot,
        COALESCE(fmt.evt_tx_index, 0) AS tx_index,
        'fourmeme' AS project
    FROM four_meme_multichain.tokenmanager2_evt_tokenpurchase fmt
    LEFT JOIN creators c ON c.token = fmt.token
    WHERE fmt.evt_block_time >= (SELECT startDate FROM constants) AND chain = 'bnb'

    UNION ALL

    SELECT 
        fmt.token,
        fmt.evt_tx_from AS wallet,
        'sell' AS swap_type,
        CASE
            WHEN c.quote = 0x0000000000000000000000000000000000000000 THEN fmt.cost / 1e18
            WHEN c.quote = 0x000Ae314E2A2172a039B26378814C252734f556A THEN fmt.cost * (SELECT aster_price FROM constants) / (SELECT bnb_price FROM constants) / 1e18
            WHEN c.quote = 0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d THEN fmt.cost / (SELECT bnb_price FROM constants) / 1e18
            WHEN c.quote = 0x55d398326f99059ff775485246999027b3197955 THEN fmt.cost / (SELECT bnb_price FROM constants) / 1e18
        END AS bnb_amount,
        fmt.amount / 1e18 AS token_amount,
        CASE
            WHEN c.quote = 0x0000000000000000000000000000000000000000 THEN fmt.price * (SELECT bnb_price FROM constants) / 1e12
            WHEN c.quote = 0x000Ae314E2A2172a039B26378814C252734f556A THEN fmt.price / (SELECT aster_price FROM constants) / 1e12
            WHEN c.quote = 0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d THEN fmt.price / 1e12
            WHEN c.quote = 0x55d398326f99059ff775485246999027b3197955 THEN fmt.price / 1e12
        END AS mc,
        fmt.evt_block_time AS block_time,
        fmt.evt_block_number AS block_slot,
        COALESCE(fmt.evt_tx_index, 0) AS tx_index,
        'fourmeme' AS project_program_id
    FROM four_meme_multichain.tokenmanager2_evt_tokensale fmt
    LEFT JOIN creators c ON c.token = fmt.token
    WHERE fmt.evt_block_time >= (SELECT startDate FROM constants) AND chain = 'bnb'

    UNION ALL

    SELECT 
        mt.token AS token,
        tx_from AS wallet,
        'buy' AS swap_type,
        CASE
            WHEN dt.token_sold_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c THEN token_sold_amount
            WHEN dt.token_sold_address = 0x0000000000000000000000000000000000000000 THEN token_sold_amount
            WHEN dt.token_sold_address = 0x000Ae314E2A2172a039B26378814C252734f556A THEN token_sold_amount * (SELECT aster_price FROM constants) / (SELECT bnb_price FROM constants)
            WHEN dt.token_sold_address = 0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d THEN token_sold_amount / (SELECT bnb_price FROM constants)
            WHEN dt.token_sold_address = 0x55d398326f99059ff775485246999027b3197955 THEN token_sold_amount / (SELECT bnb_price FROM constants)
        END AS bnb_amount,
        token_bought_amount AS token_amount,
        CAST(amount_usd * pow(10, 6) / token_bought_amount AS DOUBLE) AS mc,
        block_time,
        block_number AS block_slot,
        COALESCE(evt_index, 0) AS tx_index,
        'pancake' AS project
    FROM 
        dex.trades dt
    INNER JOIN migrations mt ON mt.token = dt.token_bought_address
    WHERE 
        block_time >= (SELECT startDate FROM constants)
        AND blockchain = 'bnb' 
        AND project = 'pancakeswap'

    UNION ALL

    SELECT 
        mt.token AS token,
        tx_from AS wallet,
        'sell' AS swap_type,
        CASE
            WHEN dt.token_bought_address = 0x0000000000000000000000000000000000000000 THEN token_bought_amount
            WHEN dt.token_bought_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c THEN token_bought_amount
            WHEN dt.token_bought_address = 0x000Ae314E2A2172a039B26378814C252734f556A THEN token_bought_amount * (SELECT aster_price FROM constants) / (SELECT bnb_price FROM constants)
            WHEN dt.token_bought_address = 0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d THEN token_bought_amount / (SELECT bnb_price FROM constants)
            WHEN dt.token_bought_address = 0x55d398326f99059ff775485246999027b3197955 THEN token_bought_amount / (SELECT bnb_price FROM constants)
        END AS bnb_amount,
        token_sold_amount AS token_amount,
        CAST(amount_usd * pow(10, 6) / token_sold_amount AS DOUBLE) AS mc,
        block_time,
        block_number AS block_slot,
        COALESCE(evt_index, 0) AS tx_index,
        'pancake' AS project
    FROM 
        dex.trades dt
    INNER JOIN migrations mt ON mt.token = dt.token_sold_address
    WHERE 
        block_time >= (SELECT startDate FROM constants)
        AND blockchain = 'bnb' 
        AND project = 'pancakeswap'
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
    WHERE swap_type = 'buy' AND bnb_amount > 0.02 AND bnb_amount < 1
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
first_buy_mc AS (
    SELECT
        wallet,
        token,
        mc AS buy_mc,
        bnb_amount AS buy_sol,
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
            bnb_amount,
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
            t.bnb_amount,
            t.block_time,
            t.block_slot,
            -- Cumulative sum of buy and sell token_amounts, ordered by block_slot
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_token,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_token,
            -- Cumulative sum of buy and sell bnb_amounts, ordered by block_slot
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.bnb_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_sol,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.bnb_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_slot ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_sol
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
            SUM(CASE WHEN t.swap_type = 'buy' THEN -t.bnb_amount ELSE t.bnb_amount END) AS tokenProfit,
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.bnb_amount ELSE 0 END) AS total_buy_sol,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.bnb_amount ELSE 0 END) AS total_sell_sol,
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
    -- LEFT JOIN dune.rnadys410_team_4024.result_fake_tokens fake ON ta.token = fake.token
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
    HAVING tw.token_count < 30
)
SELECT * FROM walletInfo
ORDER BY avg_buy_mc ASC