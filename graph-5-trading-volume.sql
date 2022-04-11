WITH all_trades AS (
    SELECT
        A.block_time,
        CONCAT(B."tokenA",'/', B."tokenB") AS trading_pair,
        A.usd_amount
    FROM dex."trades" AS A
    JOIN uniswap_v3."Factory_call_createPool" AS B
    ON A.exchange_contract_address = B.output_pool
    WHERE 
        A.project = 'Uniswap'
        AND A.version = '3'
        AND A.block_time >= '03-17-2022 00:00' AND A.block_time < '03-18-2022 12:00'
        AND B."tokenA" = '\x4d224452801aced8b2f0aebe155379bb5d594381' -- ApeCoin
)

SELECT
    DATE_TRUNC('minute', block_time) as block_time_minute,
    -- we use trading pair here insteaad of exchange_contract_address
    -- because trading pools may have different versions.
    -- example: 
    --    APE v7: \xac4b3dacb91461209ae9d41ec517c2b9cb1b7daf, 
    --    APE v4: \xf79fc43494ce8a4613cb0b2a67a1b1207fd05d27
    trading_pair,
    (CASE 
        WHEN trading_pair = '\x4d224452801aced8b2f0aebe155379bb5d594381/\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'APE/ETH'
        ELSE 'APE/USDC' 
    END) AS liquidity_pool_name,    
    COUNT(*) AS num_trades,
    SUM(usd_amount) AS total_usd
FROM all_trades
WHERE usd_amount IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1