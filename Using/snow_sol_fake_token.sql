WITH
constants AS (
    SELECT
        (NOW() - INTERVAL '30' day) AS startDate,
        7 AS mc_multiplier
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
        CASE
            WHEN 
                (CASE
                    WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN token_bought_amount
                    ELSE token_sold_amount
                END) >= 40 AND (project = 'pumpdotfun' OR project = 'raydium_launchlab' OR (project = 'meteora' AND version = 4)) THEN 420
            ELSE 
                CAST((amount_usd * pow(10, 9) / (
                    CASE
                        WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' AND token_sold_amount >= 0.001 THEN token_sold_amount
                        WHEN token_bought_mint_address != 'So11111111111111111111111111111111111111112' AND token_bought_amount >= 0.001 THEN token_bought_amount
                        ELSE 0.001
                    END
                ) / sp.price) AS DOUBLE) 
            END AS mc,
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
    LEFT JOIN sol_price_table sp ON t.block_date = sp.block_date
    WHERE
        block_time >= (SELECT startDate FROM constants)
        AND amount_usd BETWEEN 20 AND 100000
        AND token_bought_mint_address != 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        AND token_sold_mint_address != 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        AND (t.project = 'pumpdotfun' OR t.project = 'pumpswap' OR t.project = 'raydium_launchlab' OR t.project = 'raydium' OR t.project = 'meteora' AND t.version = 3 OR t.project = 'meteora' AND t.version = 4)
),
dev_sell_count AS (
    SELECT
        trades.token,
        COUNT(CASE WHEN swap_type = 'sell' AND c.dev = trades.wallet THEN 1 END) AS dev_sell_count
    FROM trades
    INNER JOIN dune.nezha_family.result_token_creators c ON trades.token = c.token
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
    GROUP BY token, project, block_slot
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
CROSS JOIN constants c
WHERE 
    (
        (project = 'pump' OR project = 'launchlab' OR project = 'meteoradbc') AND (close_mc > 0 AND open_mc > 0)
        AND 
        (close_mc - open_mc > 25 * c.mc_multiplier 
        OR open_mc - close_mc > 16 * c.mc_multiplier 
        OR (open_mc >= 16 * c.mc_multiplier AND close_mc_grouped <= 8 * c.mc_multiplier) 
        OR close_mc < 40 AND open_mc - close_mc > 9 * c.mc_multiplier)
    )

    OR 

    (
        (project = 'pump-mig' OR project = 'launchlab-mig' OR project = 'meteora-mig') AND (open_mc > 0 AND close_mc_grouped > 0)
        AND
        ((open_mc >= 50 * c.mc_multiplier AND open_mc >= close_mc_grouped * 2) OR (open_mc <= 50 * c.mc_multiplier AND open_mc - close_mc_grouped >= 30 * c.mc_multiplier))
    )

    OR dsc.dev_sell_count >= 10
GROUP BY trades_by_slot_grouped.token