-- Estimate potentail revenu if user that did not renwed since 60 days their subscription would do so.
-- Assumptions: Subscripton is for about an month
-- Finding: Based on last transaction ammount of user per game, if these user would subscribe again with same amountm then about 34 thousned PLN could be earned, and on averge it is around 214 PLN per game with subscription   

WITH subscription_users AS (
  SELECT 
    user_id,
    game_id,
    MIN(transaction_date) as first_subscription_date,
    MAX(transaction_date) as last_subscription_date,
    COUNT(*) as subscription_count
  FROM {{ ref('stg_transactions') }}
  WHERE product_type = 'Subscription'
  GROUP BY user_id, game_id
  HAVING COUNT(*) = 1
),
max_date AS (
  SELECT CAST(MAX(transaction_date) as TIMESTAMP) as max_date
  FROM {{ ref('stg_transactions') }}
),
last_subscription_by_user_and_game AS (
  SELECT 
    su.user_id,
    su.game_id,
    {{ convert_currency('t.amount', 't.currency', 't.transaction_date') }} as amount,
    t.transaction_date
  FROM {{ ref('stg_transactions') }} t

  JOIN subscription_users su 
    ON t.user_id = su.user_id 
    AND t.game_id = su.game_id 
    AND t.transaction_date = su.last_subscription_date
)

SELECT
  'Sum if all user would subscribe again' as record_type,
  ROUND(SUM(amount)::numeric, 2) as value,
  'PLN' as currency
FROM last_subscription_by_user_and_game

UNION ALL

SELECT
  'Avrage missing reveneue per game' as record_type,
  ROUND(AVG(amount)::numeric, 2) as value,
  'PLN' as currency
FROM last_subscription_by_user_and_game
