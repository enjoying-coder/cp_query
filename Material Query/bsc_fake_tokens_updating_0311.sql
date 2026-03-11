WITH
constants AS (
    SELECT
        (NOW() - INTERVAL '30' day) AS startDate,
        0.69 AS aster_price,
        640 AS bnb_price
),
migrations AS (
    SELECT 
        token,
        evt_block_time AS mig_block_time,
        evt_block_number AS mig_block_slot
    FROM 
        four_meme_multichain.tokenmanager2_evt_tradestop WHERE chain = 'bnb'

    UNION ALL

    SELECT 
        token,
        evt_block_time AS mig_block_time,
        evt_block_number AS mig_block_slot
    FROM 
        flap_bnb.portal_evt_launchedtodex
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

    UNION ALL

    SELECT
        token,
        0x0000000000000000000000000000000000000000 AS quote,
        evt_block_time AS block_time,
        creator AS dev
    FROM 
        flap_bnb.portal_evt_tokencreated
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
        fmt.evt_block_number AS block_number,
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
        fmt.evt_block_number AS block_number,
        COALESCE(fmt.evt_tx_index, 0) AS tx_index,
        'fourmeme' AS project_program_id
    FROM four_meme_multichain.tokenmanager2_evt_tokensale fmt
    LEFT JOIN creators c ON c.token = fmt.token
    WHERE fmt.evt_block_time >= (SELECT startDate FROM constants) AND chain = 'bnb'

    UNION ALL

    SELECT 
        fmt.token,
        fmt.evt_tx_from AS wallet,
        'buy' AS swap_type,
        eth / 1e18 AS bnb_amount,
        fmt.amount / 1e18 AS token_amount,
        fmt.postPrice * (SELECT bnb_price FROM constants) / 1e12 AS mc,
        fmt.evt_block_time AS block_time,
        fmt.evt_block_number AS block_slot,
        COALESCE(fmt.evt_index, 0) AS tx_index,
        'flap' AS project
    FROM flap_bnb.portal_evt_tokenbought fmt
    LEFT JOIN creators c ON c.token = fmt.token
    WHERE fmt.evt_block_time >= (SELECT startDate FROM constants)

    UNION ALL

    SELECT 
        fmt.token,
        fmt.evt_tx_from AS wallet,
        'sell' AS swap_type,
        eth / 1e18 AS bnb_amount,
        fmt.amount / 1e18 AS token_amount,
        fmt.postPrice * (SELECT bnb_price FROM constants) / 1e12 AS mc,
        fmt.evt_block_time AS block_time,
        fmt.evt_block_number AS block_slot,
        COALESCE(fmt.evt_index, 0) AS tx_index,
        'flap' AS project
    FROM flap_bnb.portal_evt_tokensold fmt
    LEFT JOIN creators c ON c.token = fmt.token
    WHERE fmt.evt_block_time >= (SELECT startDate FROM constants)


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
        block_number AS block_number,
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
        block_number AS block_number,
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
trades_by_slot AS (
    SELECT
        token,
        block_number,
        MIN_BY(mc, tx_index) AS open_mc,
        MAX_BY(mc, tx_index) AS close_mc,
        project
    FROM trades
    GROUP BY token, block_number, project
),
trades_by_slot_grouped AS (
SELECT
    token,
    project,
    block_number,
    open_mc,
    close_mc,
    LEAD(close_mc, 2) OVER (PARTITION BY token, project ORDER BY block_number) AS close_mc_grouped
FROM trades_by_slot
)

SELECT token
FROM 
    trades_by_slot_grouped 
WHERE 
    (project = 'fourmeme') AND (
        close_mc <= open_mc * 0.3
        OR close_mc - open_mc > 25
        OR open_mc - close_mc > 25
        OR (open_mc >= 20 AND close_mc_grouped <= 8))
    OR (project = 'flap') AND (
        close_mc <= open_mc * 0.3
        OR close_mc - open_mc > 20
        OR open_mc - close_mc > 20
        OR (open_mc >= 20 AND close_mc_grouped <= 8))
    OR (project = 'pancake') AND (
        (open_mc >= 50 AND open_mc >= close_mc_grouped * 3)
        OR (open_mc <= 50 AND open_mc - close_mc_grouped >= 30))
GROUP BY token