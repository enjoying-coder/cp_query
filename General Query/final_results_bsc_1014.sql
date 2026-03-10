WITH
constants AS (
    SELECT
        TIMESTAMP '2025-10-12' AS startDate,
        0 AS min_buy_mc,
        50 AS max_buy_mc,
        0 AS min_token_age,
        70000 AS max_token_age,
        0 AS min_first_buy,
        100 AS max_first_buy,
        0 AS min_total_buy,
        100 AS max_total_buy,
        3 AS rise_filter,
        120 AS rise_filter_time,
        20 AS dump_percent,
        3 AS bonding_selection_mode, -- 1: only bonding buy, 2: only after-bonding buy, 3: both
        1.9 AS aster_price,
        1270 AS bnb_price
),
token_symbol AS (
    SELECT 
        token,
        symbol
    FROM four_meme_bnb.tokenmanager2_evt_tokencreate
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
    SELECT * FROM trades_for_ath_temp WHERE rn % 3 = 1
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
        CASE WHEN fb.buy_block_number < COALESCE(tath.ath_block_number, 0) THEN 1 ELSE 0 END AS is_buy_before_ath,
        CASE WHEN ta.min_sell_time > m.mig_block_time THEN 1 ELSE 0 END AS is_sell_after_mig,
        CASE WHEN fb.buy_block_number < COALESCE(m.mig_block_number, 0) THEN 1 ELSE 0 END AS is_mig,
        CASE WHEN fb.buy_block_number >= COALESCE(m.mig_block_number, 0) - 2 AND COALESCE(m.mig_block_number, 0) > 0 THEN 1 ELSE 0 END AS is_bundle,
        fb.buy_block_number,
        COALESCE(m.mig_block_number, 0) AS mig_block_number,
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
    LEFT JOIN dune.rnadys410_team_4024.result_bsc_fake_tokens fake ON ta.token = fake.token
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
                (c.bonding_selection_mode = 1 AND first_buy_project = 'fourmeme') OR
                (c.bonding_selection_mode = 2 AND first_buy_project != 'fourmeme') OR
                c.bonding_selection_mode = 3
            ) AND
            first_buy_sol >= c.min_first_buy AND first_buy_sol <= c.max_first_buy AND
            total_buy_sol >= c.min_total_buy AND total_buy_sol <= c.max_total_buy
        ) AS is_valid
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
    WHERE is_valid
),
wallet_patterns AS (
    SELECT
        wtt.wallet,
        array_join(array_agg(ts.symbol ORDER BY wtt.ath_mc DESC), ',') AS pattern
    FROM wallet_top_tokens wtt
    LEFT JOIN token_symbol ts ON wtt.token = ts.token
    WHERE wtt.rn <= 3
    GROUP BY wtt.wallet
)
SELECT wp.wallet, wp.pattern, r.* FROM dune.pioneer990221.result_bsc_general r
INNER JOIN wallet_patterns wp ON r.wallet = wp.wallet
WHERE    r.count >= 2
    AND r.token_count < 30
    AND r.creator_count <= 1
    -- AND r.good_count >= 2
    -- AND r.bonding_count > 5
    AND r.rise_cond_met >= 30
    AND r.hold_rise_3x >= 30
    AND r.hold_rise_2x >= 40
    AND r.last_trade >= TIMESTAMP '2025-10-13'
ORDER BY r.total_metrics DESC