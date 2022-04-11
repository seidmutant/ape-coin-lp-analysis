WITH pools AS (
    SELECT
        pool AS pool_contract_address,
        token0,
        token1
    FROM uniswap_v3."Factory_evt_PoolCreated"
    WHERE (
        "token0" = '\x4d224452801aced8b2f0aebe155379bb5d594381'
        OR "token1" = '\x4d224452801aced8b2f0aebe155379bb5d594381'
    )
),

pair_event_mint AS (
    SELECT 
        date_trunc('minute', evt_block_time) as block_time_minute,
        token0,
        token1,
        18 AS decimals0,
        (CASE
            WHEN A.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' THEN 18 
            WHEN A.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' THEN 6
        ELSE 18 END) AS decimals1,        
        A.contract_address AS pool_contract_address,
        C."output_tokenId"::text AS token_id,
        SUM(amount0) as total_token_a,
        SUM(amount1) as total_token_b
    FROM uniswap_v3."Pair_evt_Mint" A
    JOIN pools AS B
        ON A.contract_address = B.pool_contract_address
    JOIN uniswap_v3."NonfungibleTokenPositionManager_call_mint" AS C
        ON A.evt_tx_hash = C.call_tx_hash
        AND A.evt_block_time = C.call_block_time
    WHERE
        A.amount > 0
        AND (
            A.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' -- APE/ETH
            OR A.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' -- APE/USDC
        )
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

pair_event_burn AS (
    SELECT 
        date_trunc('minute', evt_block_time) as block_time_minute,
        token0,
        token1,
        18 AS decimals0,
        (CASE
            WHEN A.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' THEN 18 
            WHEN A.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' THEN 6
        ELSE 18 END) AS decimals1,        
        A.contract_address AS pool_contract_address,
        (C.params->'tokenId')::text AS token_id,
        SUM(-amount0) as total_token_a,
        SUM(-amount1) as total_token_b
    FROM uniswap_v3."Pair_evt_Burn" A
    JOIN pools B
    ON A.contract_address = B.pool_contract_address
    INNER JOIN uniswap_v3."NonfungibleTokenPositionManager_call_decreaseLiquidity" C
        ON A.evt_tx_hash = C.call_tx_hash
        AND A.evt_block_time = C.call_block_time    
    WHERE
        A.amount > 0
        AND (
            A.contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' -- APE/ETH
            OR A.contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' -- APE/USDC
        )
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

pair_event_union AS (
    SELECT * FROM pair_event_mint
    UNION
    SELECT * FROM pair_event_burn
),

net_deposits AS (
    SELECT
        block_time_minute,
        token0,
        token1,
        decimals0,
        decimals1,
        pool_contract_address,
        token_id,
        sum(total_token_a) as token_a_amount,
        sum(total_token_b) as token_b_amount
    FROM pair_event_union
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

net_deposits_token_a AS (
    SELECT
        block_time_minute,
        token0 AS token_address,
        pool_contract_address,
        token_id,
        sum(sum(token_a_amount/10^decimals0)) over (partition BY pool_contract_address, token_id, token0 ORDER BY block_time_minute) as cumulative_token_balance
    FROM net_deposits AS A
    GROUP BY 1, 2, 3, 4
),

net_deposits_token_b AS (
    SELECT
        block_time_minute,
        token1 AS token_address,
        pool_contract_address,
        token_id,
        sum(sum(token_b_amount/10^decimals1)) over (partition BY pool_contract_address, token_id, token1 ORDER BY block_time_minute) as cumulative_token_balance
    FROM net_deposits 
    GROUP BY 1, 2, 3, 4
),

net_deposits_union AS (
    SELECT * FROM net_deposits_token_a
    UNION
    SELECT * FROM net_deposits_token_b
),

token_amounts AS (
    SELECT 
        block_time_minute,
        pool_contract_address,
        token_address,
        token_id,
        SUM(cumulative_token_balance) as token_balance        
    FROM net_deposits_union
    GROUP BY 1, 2, 3, 4
),

dex_trades as (
    SELECT 
        block_time,
        token_a_address,
        token_b_address,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount
    FROM dex.trades
    WHERE 
        (
            token_a_address = '\x4d224452801aced8b2f0aebe155379bb5d594381'
            OR token_b_address = '\x4d224452801aced8b2f0aebe155379bb5d594381'
        )
        AND usd_amount > 0
),

dex_prices AS (
    SELECT 
        date_trunc('minute', block_time) as block_time_minute, 
        date_trunc('hour', block_time) as block_time_hour,
        '\x4d224452801aced8b2f0aebe155379bb5d594381' AS ape_token_address,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS eth_token_address,
        '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AS usd_token_address,
        AVG(
            CASE 
                WHEN token_a_address = '\x4d224452801aced8b2f0aebe155379bb5d594381' THEN usd_amount/(token_a_amount_raw/1e18)
                WHEN token_b_address ='\x4d224452801aced8b2f0aebe155379bb5d594381' THEN usd_amount/(token_b_amount_raw/1e18) 
            END
        ) as ape_price,
        AVG(
            CASE 
                WHEN token_a_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN usd_amount/(token_a_amount_raw/1e18)
                WHEN token_b_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN usd_amount/(token_b_amount_raw/1e18) 
            END
        ) as eth_price,
        MAX(1.0) AS usd_price,
        COUNT(*) as trades 
    FROM dex_trades
    GROUP BY 1, 2, 3, 4, 5
),

prices AS (
    SELECT block_time_minute, block_time_hour, ape_token_address AS token_address, ape_price AS price 
    FROM dex_prices
    WHERE ape_price IS NOT NULL
    UNION 
    SELECT block_time_minute, block_time_hour, eth_token_address AS token_address, eth_price AS price 
    FROM dex_prices
    WHERE eth_price IS NOT NULL
    UNION 
    SELECT block_time_minute, block_time_hour, usd_token_address AS token_address, usd_price AS price 
    FROM dex_prices
    WHERE usd_price IS NOT NULL
),

generate_date_minute AS (
    SELECT 
        generate_series(DATE_TRUNC('minute',LEAST('2022-03-17 04:04',NOW()) ) - interval '1 minute', 
        date_trunc('minute', LEAST('03-30-2022',NOW()) ), '1 minute') AS block_time_minute
),

generate_date_hour AS (
    SELECT 
        generate_series(DATE_TRUNC('hour',LEAST('2022-03-17 04:04',NOW()) ) - interval '1 hour', 
        date_trunc('hour', LEAST('03-30-2022',NOW()) ), '1 hour') AS block_time_hour
),

generate_date AS (
    SELECT block_time_minute, date_trunc('hour', block_time_minute) AS block_time_hour
    FROM generate_date_minute
),

--- Work-around for token prices.
prices_generate AS (
    SELECT 
        A.block_time_minute,
        (CASE WHEN B.block_time_minute IS NULL THEN C.token_address
            ELSE B.token_address END
        ) AS token_address,
        AVG(CASE WHEN B.block_time_minute IS NULL THEN C.price
            ELSE B.price END
        ) AS price        
    FROM generate_date AS A
    LEFT JOIN prices AS B
    ON A.block_time_minute = B.block_time_minute
    LEFT JOIN prices AS C
    ON A.block_time_hour = C.block_time_hour
    GROUP BY 1, 2
),

token_amounts_with_prices AS (
    SELECT
        A.block_time_minute,
        A.pool_contract_address,
        A.token_id,
        A.token_address,
        SUM(token_balance) AS token_balance_solo,
        SUM(price) AS sum_price_solo,
        SUM(token_balance * price) as token_balance
    FROM token_amounts AS A
    JOIN prices_generate AS B
    ON 
        A.block_time_minute = B.block_time_minute 
        AND A.token_address::text = B.token_address::text
    GROUP BY 1, 2, 3, 4
),

--- total locked value
total_locked_value AS (
    SELECT
        block_time_minute,
        pool_contract_address,
        token_id,
        (CASE 
            WHEN pool_contract_address = '\xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27' THEN 'APE/ETH'
            WHEN pool_contract_address = '\xb07fe2f407f971125d4eb1977f8acee8846c7324' THEN 'APE/USDC'
            ELSE NULL
        END) AS pool_contract_name,    
        SUM(token_balance) total_TVL
    FROM token_amounts_with_prices
    GROUP BY 1, 2, 3, 4
)

SELECT * 
FROM total_locked_value
WHERE 
    block_time_minute < '2022-03-18T12:00:00+00:00'
    AND total_tvl > 0
ORDER BY block_time_minute