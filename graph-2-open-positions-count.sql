WITH liquidity_mint AS (
    SELECT
        A.call_block_time,
        A.amount,
        A.contract_address,
        B."output_tokenId"::text AS token_id
    FROM uniswap_v3."Pair_call_mint" A
    JOIN uniswap_v3."NonfungibleTokenPositionManager_call_mint" B
        ON A.call_tx_hash = B.call_tx_hash
        AND A.call_block_time = B.call_block_time
    WHERE
        B."output_tokenId"::text IS NOT NULL
        AND A.amount > 0
        AND (
            A.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' -- APE/ETH
            OR A.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' -- APE/USDC
        )
        AND A.call_success = 'true' 
        AND A.call_success = 'true'
),

liquidity_increase AS (
    SELECT
        A.call_block_time,
        A.amount, 
        A.contract_address,
        (B.params->'tokenId')::text AS token_id 
    FROM uniswap_v3."Pair_call_mint" A
    JOIN uniswap_v3."NonfungibleTokenPositionManager_call_increaseLiquidity" B
        ON A.call_tx_hash = B.call_tx_hash
        AND A.call_block_time = B.call_block_time
    WHERE 
        (B.params->'tokenId')::text IS NOT NULL
        AND A.amount > 0
        AND (
            A.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' -- APE/ETH
            OR A.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' -- APE/USDC
        )
        AND A.call_success = 'true' 
        AND B.call_success = 'true'
),

liquidity_decrease AS (
    SELECT 
        A.call_block_time,
        (-1)*A.amount AS amount, 
        A.contract_address,
        (B.params->'tokenId')::text AS token_id
    FROM uniswap_v3."Pair_call_burn" A
    INNER JOIN uniswap_v3."NonfungibleTokenPositionManager_call_decreaseLiquidity" B
        ON A.call_tx_hash = B.call_tx_hash
        AND A.call_block_time = B.call_block_time
    WHERE 
        (B.params->'tokenId')::text IS NOT NULL
        AND A.amount > 0
        AND (
            A.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' -- APE/ETH
            OR A.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' -- APE/USDC
        )
        AND A.call_success = 'true'
        AND B.call_success = 'true'
),

liquidity_pool_combined AS (
    SELECT * FROM liquidity_mint
    UNION
    SELECT * FROM liquidity_increase
    UNION
    SELECT * FROM liquidity_decrease
),

liquidity_pool_final_snapshot AS (
    SELECT 
        token_id, 
        contract_address,
        SUM(amount) AS total_amount, 
        MIN(call_block_time) AS min_block_time,
        MAX(call_block_time) AS max_block_time
    FROM liquidity_pool_combined
    GROUP BY 1, 2
),

liquidity_pool_total_amount AS (
    SELECT 
        contract_address,
        max_block_time, 
        min_block_time,
        SUM(total_amount) AS total_amount, 
        COUNT(*) AS unique_open_positions
    FROM liquidity_pool_final_snapshot
    GROUP BY 1, 2, 3
),

generate_date_minute AS (
    SELECT 
        generate_series(DATE_TRUNC('minute',LEAST('2022-03-17 04:04',NOW()) ) - interval '1 minute', 
        date_trunc('minute', LEAST('03-30-2022',NOW()) ), '1 minute') AS date_minute
)

SELECT
    A.date_minute, 
    (CASE 
        WHEN B.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' THEN 'APE/ETH'
        WHEN B.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' THEN 'APE/USDC'
        ELSE NULL
    END) AS contract_name,
    SUM(
        CASE WHEN
            (A.date_minute >= min_block_time AND A.date_minute <= max_block_time AND total_amount = 0) 
            OR (A.date_minute >= min_block_time AND total_amount > 1)
        THEN 1 ELSE 0 END
    ) AS count_open_positions
FROM generate_date_minute AS A
CROSS JOIN liquidity_pool_total_amount AS B
GROUP BY 1, 2