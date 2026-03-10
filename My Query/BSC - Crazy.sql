WITH
constants AS (
    SELECT
        TIMESTAMP '2025-10-27' AS startDate,
        0 AS min_buy_mc,
        50 AS max_buy_mc,
        0 AS min_token_age,
        10000 AS max_token_age,
        0 AS min_first_buy,
        100 AS max_first_buy,
        0 AS min_total_buy,
        100 AS max_total_buy,
        3 AS rise_filter,
        60 AS rise_filter_time,
        20 AS dump_percent,
        3 AS bonding_selection_mode, -- 1: only bonding buy, 2: only after-bonding buy, 3: both
        1.15 AS aster_price,
        1096 AS bnb_price
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
trades_for_ath_temp AS (
    SELECT 
        token, 
        MIN_BY(mc, tx_index) AS mc, 
        MIN_BY(block_time, tx_index) AS block_time, 
        MIN(tx_index) AS tx_index,
        block_number,
        ROW_NUMBER() OVER (PARTITION BY token, block_number ORDER BY MIN(tx_index) ASC) AS rn
    FROM trades
    WHERE swap_type = 'buy' AND bnb_amount > 0.02 AND bnb_amount < 1
    GROUP BY token, block_number
),
trades_for_ath AS (
    SELECT * FROM trades_for_ath_temp  WHERE rn % 3 = 1
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
        FROM trades
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
            t.bnb_amount,
            t.block_time,
            t.block_number,
            -- Cumulative sum of buy and sell token_amounts, ordered by block_number
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_token,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_token,
            -- Cumulative sum of buy and sell bnb_amounts, ordered by block_number
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.bnb_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_buy_sol,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.bnb_amount ELSE 0 END) OVER (PARTITION BY t.wallet, t.token ORDER BY t.block_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sell_sol
        FROM trades t
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
                WHEN cum_sell_token >= cum_buy_token * 0.95 AND cum_buy_token > 0 AND cum_sell_token < cum_buy_token * 1.05THEN
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
            COUNT_IF(t.swap_type = 'buy') AS buy_count,
            COUNT_IF(t.swap_type = 'sell') AS sell_count,
            SUM(CASE WHEN t.swap_type = 'buy' THEN t.token_amount ELSE 0 END) AS total_buy_token,
            SUM(CASE WHEN t.swap_type = 'sell' THEN t.token_amount ELSE 0 END) AS total_sell_token,
            MIN(CASE WHEN t.swap_type = 'sell' THEN t.block_time END) AS min_sell_time,
            MIN(CASE WHEN t.swap_type = 'buy' THEN t.block_time END) AS min_buy_time,
            MIN(t.block_time) AS firstTradeTime,
            MIN(t.block_number) AS firstTradeSlot
        FROM trades t
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
            WHEN fvs.valid_sell_time IS NULL THEN 1800
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
        CASE WHEN fb.buy_block_number < COALESCE(tath.ath_block_number, 0) THEN 1 ELSE 0 END AS is_buy_before_ath,
        CASE WHEN ta.min_sell_time > m.mig_block_time THEN 1 ELSE 0 END AS is_sell_after_mig,
        CASE WHEN fb.buy_block_number < COALESCE(m.mig_block_number, 0) THEN 1 ELSE 0 END AS is_mig,
        CASE WHEN fb.buy_block_number >= COALESCE(m.mig_block_number, 0) - 2 AND COALESCE(m.mig_block_number, 0) > 0 THEN 1 ELSE 0 END AS is_bundle,
        fb.buy_block_number,
        COALESCE(m.mig_block_number, 0) AS mig_block_number,
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
    LEFT JOIN dune.rnadys410_team_4024.result_bsc_fake_tokens fake ON ta.token = fake.token
    LEFT JOIN tokens_ath tath ON ta.token = tath.token
    LEFT JOIN migrations m ON ta.token = m.token
    LEFT JOIN creators c ON ta.token = c.token
    LEFT JOIN trades_by_slot tbs ON ta.token = tbs.token AND ta.firstTradeSlot = tbs.block_number
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
    JOIN trades_for_ath t ON t.token = itt.token AND t.block_time > itt.firstTradeTime AND t.block_time < itt.firstTradeTime + INTERVAL '30' minute
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
                (c.bonding_selection_mode = 1 AND first_buy_project = 'fourmeme') OR
                (c.bonding_selection_mode = 2 AND first_buy_project != 'fourmeme') OR
                c.bonding_selection_mode = 3
            ) AND
            first_buy_sol >= c.min_first_buy AND first_buy_sol <= c.max_first_buy AND
            total_buy_sol >= c.min_total_buy AND total_buy_sol <= c.max_total_buy
        ) AS is_valid,
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
        SUM(CASE WHEN is_valid THEN total_buy_sol ELSE 0 END) AS buy_sol,
        SUM(CASE WHEN is_valid THEN total_sell_sol ELSE 0 END) AS sell_sol,
        SUM(CASE WHEN is_valid THEN tokenProfit ELSE 0 END) AS totalProfit,
        SUM(CASE WHEN is_valid THEN LN(pnl + 1) / LN(2) ELSE 0 END) / COUNT_IF(is_valid) AS pnl_log,
        SUM(CASE WHEN is_valid THEN LN(hold_pnl + 1) / LN(2) ELSE 0 END) / COUNT_IF(is_valid) AS hold_pnl_log,
        SUM(CASE WHEN is_valid THEN LN(ath_mc / first_buy_mc) / LN(2) ELSE 0 END) / COUNT_IF(is_valid) AS rise_log,
        SUM(CASE WHEN is_valid THEN LN(valid_rise) / LN(2) ELSE 0 END) / COUNT_IF(is_valid) AS hold_rise_log,
        (SUM(CASE WHEN is_valid THEN total_sell_sol ELSE 0 END) - SUM(CASE WHEN is_valid THEN total_buy_sol ELSE 0 END)) 
            / NULLIF(SUM(CASE WHEN is_valid THEN total_buy_sol ELSE 0 END), 0) AS totalpnl,
        COUNT_IF(is_valid) AS count,
        COUNT_IF(is_valid AND (buy_count > 3 OR hold_time < 10 OR buy_rise > 1.4)) AS bad_count,
        COUNT_IF(is_valid AND is_bundle = 1) AS bundle_count,
        COUNT_IF(is_valid AND first_rise_duration >= 0) AS rise_rise,
        COUNT_IF(is_valid AND first_rise_duration >= 0 AND first_rise_duration <= (SELECT rise_filter_time FROM constants)) AS rise_rise_filter,
        COUNT_IF(is_valid AND first_dump_duration < valid_sell_duration) AS dump_count,
        COUNT_IF(is_valid AND first_2x_duration >= 0) AS rise_2x,
        COUNT_IF(is_valid AND first_3x_duration >= 0) AS rise_3x,
        COUNT_IF(is_valid AND first_5x_duration >= 0) AS rise_5x,
        COUNT_IF(is_valid AND (first_3x_duration > 0 AND first_3x_duration < 600)) AS good_count,
        COUNT_IF(is_valid AND first_dump_duration = -1 OR (first_2x_duration >= 0 AND first_2x_duration < first_dump_duration)) AS safe_count,
        SUM(CASE WHEN is_valid THEN valid_rise ELSE 0 END) / COUNT_IF(is_valid) AS hold_rise_avg,
        COUNT_IF(is_valid AND valid_rise >= 2) AS hold_rise_2x,
        COUNT_IF(is_valid AND valid_rise >= 3) AS hold_rise_3x,
        COUNT_IF(is_valid AND valid_rise >= 5) AS hold_rise_5x,
        SUM(CASE WHEN is_valid AND first_2x_duration >= 0 THEN first_2x_duration ELSE 0 END) AS rise_2x_time,
        SUM(CASE WHEN is_valid AND first_3x_duration >= 0 THEN first_3x_duration ELSE 0 END) AS rise_3x_time,
        SUM(CASE WHEN is_valid AND first_5x_duration >= 0 THEN first_5x_duration ELSE 0 END) AS rise_5x_time,
        SUM(CASE WHEN is_valid AND first_rise_duration >= 0 THEN first_rise_duration ELSE 0 END) AS rise_rise_time,
        COUNT_IF(is_valid AND valid_rise >= 4 AND hold_pnl <= 1.5 AND is_buy_before_ath = 1) AS fake_seller,
        COUNT_IF(is_valid AND pnl >= 0.8) AS pnl_1x,
        COUNT_IF(is_valid AND pnl >= 2) AS pnl_2x,
        COUNT_IF(is_valid AND pnl >= 3) AS pnl_3x,
        COUNT_IF(is_valid AND is_mig = 1) AS mig_count,
        SUM(CASE WHEN is_valid AND is_mig = 1 THEN mig_time ELSE 0 END) AS mig_time,
        COUNT_IF(is_valid AND is_buy_before_ath = 1) AS buy_before_ath_count,
        COUNT_IF(is_valid AND is_sell_after_mig = 1) AS sell_after_mig_count,
        COUNT(DISTINCT ft.token) AS token_count,
        COUNT_IF(first_buy_project = 'fourmeme') AS bonding_count,
        COUNT_IF(is_valid AND is_fake = 1) AS fake_count,
        COUNT_IF(is_valid AND is_creator = 1) AS creator_count,
        COUNT_IF(is_valid AND buy_rise > 1.3) AS follow_count,
        SUM(CASE WHEN is_valid THEN (hold_pnl_metrics * 2 + valid_rise_metrics * 2 + hold_time_metrics + pnl_metrics + rise_metrics / 2 + buy_before_ath_metrics * 2 + buy_rise_metrics * 2) / 12.5 ELSE 0 END) AS total_metrics,
        SUM(CASE WHEN is_valid AND dev_metrics > 0 THEN dev_metrics ELSE 0 END) - 
            (2 * pow(2, COUNT_IF(is_valid AND dev_metrics < 0)) -1) AS devs_metrics,
        CASE 
            WHEN SUM(CASE WHEN is_valid THEN 1.0 / NULLIF(valid_rise,0) ELSE 0 END) > 0
            THEN COUNT_IF(is_valid) / SUM(CASE WHEN is_valid THEN 1.0 / NULLIF(valid_rise,0) ELSE 0 END)
            ELSE NULL
        END AS harmonic_mean_calc_pnl
    FROM filteredTokens ft
    GROUP BY ft.wallet
),
results AS (
    SELECT
        wi.wallet,
        token_count,
        bonding_count,
        count,
        -- follow_count,
        -- dump_count,
        -- good_count,
        -- safe_count,
        -- bad_count,
        mig_count,
        -- fake_seller,
        fake_count AS scam,
        last_trade,
        CASE WHEN count > 0 THEN mig_time / count ELSE 0 END AS avg_mig_time,
        hold_rise_avg,  
        CASE WHEN count > 0 THEN hold_rise_2x * 100.0 / count ELSE 0 END AS hold_rise_2x,
        CASE WHEN count > 0 THEN hold_rise_3x * 100.0 / count ELSE 0 END AS hold_rise_3x,
        CASE WHEN count > 0 THEN hold_rise_5x * 100.0 / count ELSE 0 END AS hold_rise_5x,
        CASE WHEN count > 0 THEN rise_rise * 100.0 / count ELSE 0 END AS rise_cond,
        CASE WHEN count > 0 THEN rise_rise_filter * 100.0 / count ELSE 0 END AS rise_cond_met,
        CASE WHEN count > 0 THEN rise_2x * 100.0 / count ELSE 0 END AS rise_2x,
        CASE WHEN rise_2x > 0 THEN rise_2x_time / rise_2x ELSE 0 END AS r_2x_time,
        CASE WHEN count > 0 THEN rise_3x * 100.0 / count ELSE 0 END AS rise_3x,
        CASE WHEN rise_3x > 0 THEN rise_3x_time / rise_3x ELSE 0 END AS r_3x_time,
        CASE WHEN count > 0 THEN rise_5x * 100.0 / count ELSE 0 END AS rise_5x,
        CASE WHEN rise_5x > 0 THEN rise_5x_time / rise_5x ELSE 0 END AS r_5x_time,
        CASE WHEN count > 0 THEN pnl_2x * 100.0 / count ELSE 0 END AS pnl_2x,   
        CASE WHEN count > 0 THEN pnl_3x * 100.0/ count ELSE 0 END AS pnl_3x,
        CASE WHEN count > 0 THEN bundle_count * 100.0 / count ELSE 0 END AS bundle_ratio,
        CASE WHEN count > 0 THEN mig_count * 100.0 / count ELSE 0 END AS mig_ratio,
        CASE WHEN count > 0 THEN buy_before_ath_count * 100.0 / count ELSE 0 END AS buy_before_ath,
        CASE WHEN count > 0 THEN sell_after_mig_count * 100.0 / count ELSE 0 END AS sell_after_mig,
        CASE WHEN count > 0 THEN pnl_1x * 100.0 / count ELSE 0 END AS win_rate,
        totalProfit,
        buy_sol,
        sell_sol,
        totalpnl,
        pnl_log,
        rise_log,
        hold_rise_log,
        hold_pnl_log,
        creator_count,
        CASE WHEN count > 0 THEN creator_count * 100.0 / count ELSE 0 END AS creator_ratio,
        CASE WHEN count > 0 THEN total_metrics / count ELSE 0 END AS total_metrics,
        CASE WHEN count > 0 THEN devs_metrics / count ELSE 0 END AS dev_metrics,
        harmonic_mean_calc_pnl
    FROM walletInfo wi
)
SELECT
    r.*,
    ROW_NUMBER() OVER (ORDER BY r.total_metrics DESC) AS row_num
FROM results r
WHERE 
    r.count >= 2
    AND r.count / r.token_count >= 0.3
    AND r.token_count <= 30
    AND r.creator_count <= 0
    AND r.rise_cond_met >= 30
    AND r.rise_cond >= 50
    AND r.rise_2x >= 50
    AND r.rise_3x >= 30
    -- AND r.count * 100 / r.token_count >= 50
    -- AND r.first_trade >= TIMESTAMP '2025-09-20'
ORDER BY r.rise_log DESC