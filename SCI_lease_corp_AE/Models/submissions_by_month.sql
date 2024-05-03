-- Dealer1 CTE 
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






-- SUBMISSION_TRANSACTIONS CTE 

/* used to compile submissions with all required fields including the following:
submissionID
submissionDate
salesPerson
dealerKey
dealerName*/

SubmissionTransactions AS (
SELECT 
s.submission_id as submissionID,
s.submission_date as submissionDate,
d1.salesPerson,
a.dealer_key as dealerKey,
d1.dealerName
FROM Dealer1 as d1
FULL JOIN core.application AS a
on d1.dealerKey = a.dealer_key
FULL JOIN core.submission_history AS s
on s.application_id = a.application_id

),






-- C1 CTE 
-- get submissions count for current month
C1 AS(SELECT
D1.dealerKey,
D1.dealerName,
D1.salesPerson,
COUNT(ST.submissionDate) as currentMonthCount
FROM Dealer1 AS D1
LEFT JOIN SubmissionTransactions as ST
ON D1.dealerName = ST.dealerName
WHERE MONTH(ST.submissionDate) = MONTH(GETDATE()) - 2 OR MONTH(ST.submissionDate) IS NULL
GROUP BY D1.dealerKey, D1.dealerName, D1.salesPerson),





-- C2 CTE 
-- get submissions count for previous month
C2 AS(SELECT
D1.dealerKey,
D1.dealerName,
D1.salesPerson,
COUNT(ST.submissionDate) as lastMonthCount
FROM DEALER1 AS D1
LEFT JOIN SubmissionTransactions as ST
ON D1.dealerName = ST.dealerName
WHERE MONTH(ST.submissionDate) = MONTH(GETDATE()) - 3 OR MONTH(ST.submissionDate) IS NULL
GROUP BY D1.dealerKey, D1.dealerName, D1.salesPerson),




-- C3 CTE 
-- get submissions count for month before last month
C3 AS(SELECT
D1.dealerKey,
D1.dealerName,
D1.salesPerson,
COUNT(ST.submissionDate) as twoMonthsAgoCount
FROM DEALER1 AS D1
LEFT JOIN SubmissionTransactions as ST
ON D1.dealerName = ST.dealerName
WHERE MONTH(ST.submissionDate) = MONTH(GETDATE()) - 4 OR MONTH(ST.submissionDate) IS NULL
GROUP BY D1.dealerKey, D1.dealerName, D1.salesPerson),



-- SUBSBYMONTH CTE 
-- dealerKey and Name along with submissions count for previous 3 months

SUBSBYMONTH as
(SELECT
D1.dealerKey,
D1.dealerName,
D1.salesPerson,
CASE
    WHEN C1.currentMonthCount IS NULL THEN 0
    ELSE C1.currentMonthCount END AS currentMonthCount,
CASE
    WHEN C2.lastMonthCount IS NULL THEN 0
    ELSE C2.lastMonthCount END AS lastMonthCount,
CASE
    WHEN C3.twoMonthsAgoCount IS NULL THEN 0
    ELSE C3.twoMonthsAgoCount END AS twoMonthsAgoCount
FROM Dealer1 AS D1
FULL JOIN C1
ON D1.dealerKey = C1.dealerKey
FULL JOIN C2
ON D1.dealerKey = C2.dealerKey
FULL JOIN C3
ON D1.dealerKey = C3.dealerKey),


 


-- DEALERSTATUS CTE
-- activity status of each dealer along with salesPerson and DealerName

DEALERSTATUS AS(
SELECT
dealerKey,
dealerName,
salesPerson,
CASE 
    WHEN currentMonthCount >= 3 AND lastMonthCount >= 3 AND twoMonthsAgoCount >= 3 THEN 'Consistently Active'
    WHEN currentMonthCount < 3 AND lastMonthCount >= 5 AND twoMonthsAgoCount >= 5 THEN 'Require In Person Visit'
    WHEN currentMonthCount = 0 AND lastMonthCount = 0 AND twoMonthsAgoCount =  0 THEN 'Zero Applications'
    ELSE 'Mildly Active'
    END as status
FROM SUBSBYMONTH),


--  The percentage of dealers in each salesperson's dealer base that are "consistently active"

t1 as (SELECT
DS.salesPerson,
count(DS.dealerName) as totalDealers
from DEALERSTATUS as DS
group by salesPerson),

t2 as (SELECT
DS.salesPerson,
count(DS.dealerName) as activeDealers
FROM DEALERSTATUS as DS
WHERE DS.status = 'Consistently Active'
GROUP BY salesPerson),

ActiveDealerPercentage as (SELECT
t1.*,
t2.activeDealers,
CONCAT(CAST(t2.activeDealers * 100 / t1.totalDealers AS int), '%') as PercentOfActiveDealers
FROM t1 JOIN t2
ON t1.salesPerson = t2.salesPerson)

SELECT
*
FROM ActiveDealerPercentage