WITH dex_trades as (
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
        AVG(
            CASE 
                WHEN token_a_address = '\x4d224452801aced8b2f0aebe155379bb5d594381' THEN usd_amount/(token_a_amount_raw/1e18)
                WHEN token_b_address='\x4d224452801aced8b2f0aebe155379bb5d594381' THEN usd_amount/(token_b_amount_raw/1e18) 
            END
        ) as price,
        COUNT(*) as trades 
    FROM dex_trades
    GROUP BY 1
) 
 
SELECT block_time_minute, price, trades
FROM dex_prices
ORDER BY 1