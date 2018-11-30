
-- Exercice 4 - Group-By
-- Date et State
SELECT Order_Date,
       State,
       SUM((Sales * Quantity)) as sales_amount
FROM `superstore`
GROUP BY Order_Date, State;

-- Date et Category
SELECT Order_Date,
       Category,
       SUM((Sales * Quantity)) as sales_amount
FROM `superstore`
GROUP BY Order_Date, State;

-- Produits distincts et nombre d'exemplaires
SELECT Order_ID,
       COUNT(DISTINCT Product_ID),
       SUM((Sales * Quantity)) as sales_amount
FROM `superstore`
GROUP BY Order_ID;

-- Exercice 5 - Join
-- Join Customer Name et Order Comment
SELECT C.name,
       O.comment
FROM customers C
  INNER JOIN orders O
    ON C.custkey = O.custkey;

-- Exercice 6 - Group-By + Join
-- Join Customer Name et Order SUM(totalprice)
SELECT C.name,
       SUM(O.totalprice)
FROM customers C
  INNER JOIN orders O
    ON C.custkey = O.custkey
GROUP BY C.custkey;

