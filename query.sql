SELECT tbl1.customerId, tbl1.customerName, tbl1.segment, tbl1.country, quantityOfOrdersLast5Days, totalQuantityOfOrders
FROM(SELECT customerId, customerName, segment, country, sum(five_days) AS quantityOfOrdersLast5Days FROM(
      SELECT *,
      CASE WHEN orderDate >= CURRENT_DATE - 1555
      THEN 1
      ELSE 0
      END AS five_days
      FROM customers) AS x
      GROUP BY customerId, customerName, segment, country) AS tbl1
JOIN (
  SELECT customerId, customerName, segment, country, count(1) AS totalQuantityOfOrders
  FROM(SELECT * FROM customers) AS x
  GROUP BY customerId, customerName, segment, country) AS tbl2
ON tbl1.customerId = tbl2.customerId
