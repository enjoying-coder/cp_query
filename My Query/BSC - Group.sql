WITH
constants AS (
    SELECT
        TIMESTAMP '2025-10-30' AS startDate,
        2 AS atLeastCount,
        ARRAY[0x409b30acf2767038170b71342284093b61d84444, 0xd90a5b93abc2bc13dc13b3c61a07aaea4a7d4444, 0xae9beedbc37a4e4dd3eb493ad9e489f6e2ac4444] AS group_tokens,
        2 AS aster_price,
        1320 AS bnb_price
),
migrations AS (
    SELECT 
        token,
        evt_block_time AS mig_block_time,
        evt_block_number AS mig_block_number
    FROM 
        four_meme_bnb.tokenmanager2_evt_tradestop
),
creators AS (
    SELECT 
        tc.token AS token,
        0x0000000000000000000000000000000000000000 AS quote,
        tc.evt_block_time AS block_time,
        tc.creator AS dev
    FROM 
        four_meme_bnb.tokenmanager2_evt_tokencreate tc
    -- LEFT JOIN four_meme_bnb.tokenmanager2_evt_liquidityadded la ON tc.token = la.base
),
memeTokens AS (
    SELECT
        t1.token,
        ARBITRARY(t1.totalSupply) AS totalSupply
    FROM
        four_meme_bnb.tokenmanager2_evt_tokencreate t1
        JOIN bnb.logs t2 ON t1.evt_tx_hash = t2.tx_hash
    WHERE
        t1.evt_block_time >= (
            SELECT
                startDate
            FROM
                constants
        )
        AND t2.block_date >= (
            SELECT
                startDate
            FROM
                constants
        )
    GROUP BY
        t1.token
    HAVING
        COUNT(t2.tx_hash) <= 10
),
pancakeswapTrades AS (
    SELECT
        (
            CASE
                WHEN token_bought_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c THEN token_sold_address
                ELSE token_bought_address
            END
        ) AS token,
        tx_from AS wallet,
        (
            CASE
                WHEN token_bought_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c THEN 'sell'
                ELSE 'buy'
            END
        ) AS swap_type,
        (
            CASE
                WHEN token_bought_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c THEN token_bought_amount
                ELSE token_sold_amount
            END
        ) AS bnb_amount,
        (
            CASE
                WHEN token_bought_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c THEN token_sold_amount
                ELSE token_bought_amount
            END
        ) AS token_amount,
        amount_usd,
        block_time,
        block_number,
        tx_hash,
        'pancake' AS project,
        COALESCE(evt_index, 0) AS tx_index
    FROM
        pancakeswap.trades
    WHERE
        blockchain = 'bnb'
        AND (
            token_bought_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
            OR token_sold_address = 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
        )
        AND block_time > (
            SELECT
                startDate
            FROM
                constants
        )
),
fourmemeTrades AS (
    SELECT
        token,
        account AS wallet,
        'buy' AS swap_type,
        CASE WHEN cost / POWER(10, 18) > 20
            THEN cost / POWER(10, 18) / 1160 -- USD1 Pair
            ELSE cost / POWER(10, 18) -- BNB Pair
        END as bnb_amount,
        amount / POWER(10, 18) as token_amount,
        CASE WHEN cost / POWER(10, 18) > 20
            THEN cost / POWER(10, 18) -- USD1 Pair
            ELSE cost / POWER(10, 18) * 1160 -- BNB Pair
        END as amount_usd,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        'fourmeme' AS project,
        COALESCE(evt_tx_index, 0) AS tx_index
    FROM
        four_meme_bnb.tokenmanager2_evt_tokenpurchase
    WHERE
        evt_block_time > (
            SELECT
                startDate
            FROM
                constants
        )

    UNION ALL

    SELECT
        token,
        account AS wallet,
        'sell' AS swap_type,
        CASE WHEN cost / POWER(10, 18) > 20
            THEN cost / POWER(10, 18) / 1000 -- USD1 Pair
            ELSE cost / POWER(10, 18) -- BNB Pair
        END as bnb_amount,
        amount / POWER(10, 18) as token_amount,
        CASE WHEN cost / POWER(10, 18) > 20
            THEN cost / POWER(10, 18) -- USD1 Pair
            ELSE cost / POWER(10, 18) * 1160 -- BNB Pair
        END as amount_usd,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        'fourmeme' AS project,
        COALESCE(evt_tx_index, 0) AS tx_index
    FROM
        four_meme_bnb.tokenmanager2_evt_tokensale
    WHERE
        evt_block_time > (
            SELECT
                startDate
            FROM
                constants
        )
),
trades AS (
    WITH tempTrades AS (
        SELECT
            token,
            wallet,
            swap_type,
            bnb_amount,
            token_amount,
            tx_hash,
            project,
            block_time,
            block_number,
            amount_usd,
            tx_index
        FROM (
            SELECT * FROM fourmemeTrades
            UNION ALL
            SELECT * FROM pancakeswapTrades
        )
    )
    SELECT 
        tempTrades.*,
        CAST(amount_usd / token_amount AS DOUBLE) * memeTokens.totalSupply / POWER(10, 18 + 3) AS mc
    FROM tempTrades
    -- INNER JOIN creators ct ON tempTrades.token = ct.token
    INNER JOIN memeTokens ON tempTrades.token = memeTokens.token
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
        block_number,
        FIRST_VALUE(mc) OVER w AS open_mc,
        LAST_VALUE(mc) OVER w AS close_mc
    FROM (
        SELECT
            token,
            block_number,
            mc,
            tx_index,
            ROW_NUMBER() OVER (PARTITION BY token, block_number ORDER BY tx_index ASC) AS rn_asc,
            ROW_NUMBER() OVER (PARTITION BY token, block_number ORDER BY tx_index DESC) AS rn_desc
        FROM group_tokens_trades
    ) t
    WHERE rn_asc = 1 OR rn_desc = 1
    WINDOW w AS (PARTITION BY token, block_number ORDER BY tx_index ASC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
trades_by_slot AS (
    SELECT
        tbs.token,
        tbs.block_number,
        tbs.open_mc,
        tbs.close_mc,
        LEAD(tbs.open_mc) OVER (PARTITION BY tbs.token ORDER BY tbs.block_number) AS next_open_mc,
        LEAD(tbs.close_mc) OVER (PARTITION BY tbs.token ORDER BY tbs.block_number) AS next_close_mc
    FROM (
        SELECT DISTINCT token, block_number, open_mc, close_mc
        FROM trades_by_slot_base
    ) tbs
),
trades_for_ath AS (
    SELECT token, mc, block_time, block_number
    FROM group_tokens_trades
    WHERE swap_type = 'buy' AND bnb_amount > 0.02 AND bnb_amount < 1
),
tokens_ath AS (
    SELECT
        token,
        mc AS ath_mc,
        block_time AS ath_block_time,
        block_number AS ath_block_number
    FROM (
        SELECT
            token,
            mc,
            block_time,
            block_number,
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
        block_number AS buy_block_number
    FROM (
        SELECT
            wallet,
            token,
            mc,
            project,
            block_time,
            block_number,
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
            t.block_number,
            -- Cumulative sum of buy and sell token_amounts, ordered by block_number
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_token,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_token,
            -- Cumulative sum of buy and sell bnb_amounts, ordered by block_number
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.bnb_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_sol,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.bnb_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_sol
        FROM group_tokens_trades t
    ),
    valid_sell_points AS (
        SELECT
            wallet,
            token,
            block_time AS valid_sell_time,
            cum_buy_sol AS valid_buy_sol,
            cum_sell_sol AS valid_sell_sol,
            block_number,
            ROW_NUMBER() OVER (PARTITION BY wallet, token ORDER BY block_number ASC) AS rn,
            -- Find the first point where cumulative buy token = cumulative sell token
            CASE 
                WHEN cum_sell_token >= cum_buy_token * 0.95 AND cum_buy_token > 0 THEN
                    ROW_NUMBER() OVER (
                        PARTITION BY wallet, token
                        ORDER BY block_number ASC
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
            MIN(t.block_number) AS firstTradeSlot
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
    LEFT JOIN trades_by_slot tbs ON ta.token = tbs.token AND ta.firstTradeSlot = tbs.block_number
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