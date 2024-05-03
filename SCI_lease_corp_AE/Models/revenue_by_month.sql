-- DEALER1 CTE 
--used to correct salespersons names

WITH Dealer1 AS (SELECT
dealer_key as dealerKey,
DealerName as dealerName,
case
    WHEN salesperson LIKE 'Jim%' then 'Jim Vogler'
    WHEN salesperson LIKE 'Man%' then 'Mansur Naser'
    WHEN salesperson LIKE 'N%' then 'Nora Palermo'
    WHEN salesperson LIKE 'R%' then 'Rachel Arellano'
    WHEN salesperson LIKE 'S%' then 'Samin Gupta'
    END AS salesPerson
FROM core.dealer),




-- ApplicationRevenue CTE
-- 

ApplicationRevenue AS(
SELECT
a.application_id as applicationID,
a.dealer_key as dealerKey,
MONTH(s.submission_date) AS month,
s.revenue,
d1.salesPerson
FROM core.application as a
JOIN core.submission_history as s
ON a.application_id = s.application_id
JOIN DEALER1 AS d1
ON a.dealer_key = d1.dealerKey
WHERE status = 'Approved'),

DealerRev AS (SELECT
salesPerson,
dealerKey,
month,
avg(revenue) as averageRevenue
FROM APPLICATIONREVENUE
GROUP BY salesPerson, dealerKey, month),

APR as (SELECT
salesPerson,
dealerKey,
averageRevenue as avgAPRRevenue
FROM DealerRev
WHERE month = 4),

MAY as (SELECT
salesPerson,
dealerKey,
averageRevenue as avgMAYRevenue
FROM DealerRev
WHERE month = 5),


JUN as (SELECT
salesPerson,
dealerKey,
averageRevenue as avgJUNRevenue
FROM DealerRev
WHERE month = 6),


JUL as (SELECT
salesPerson,
dealerKey,
averageRevenue as avgJULRevenue
FROM DealerRev
WHERE month = 5),

RevenueByMonth AS (SELECT 
APR.*,
MAY.avgMAYRevenue,
JUN.avgJUNRevenue,
JUL.avgJULRevenue
FROM APR
JOIN MAY
ON APR.salesPerson = MAY.salesPerson AND APR.dealerKey = MAY.dealerKey
JOIN JUN
ON APR.salesPerson = JUN.salesPerson AND APR.dealerKey = JUN.dealerKey
JOIN JUL
ON APR.salesPerson = JUL.salesPerson AND APR.dealerKey = JUL.dealerKey)


SELECT
*
FROM RevenueByMonth