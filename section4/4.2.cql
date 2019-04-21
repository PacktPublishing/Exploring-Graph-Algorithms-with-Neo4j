/*
Using the northwind dataset

To load the data, follow the instructions from:
*/
:play northwind graph

/*Product frequently bought together recommendation
*/
MATCH (p:Product)<-[:ORDERS]-(o:Order)-[:ORDERS]->(q:Product)
WHERE p.productName = "Chai"
WITH q.productName as productName, count(o) as numberOfCommonOrders
RETURN productName, numberOfCommonOrders
ORDER BY numberOfCommonOrders DESC
LIMIT 10

/*Products bought by customers in the same country
*/
MATCH (c:Customer)-[:PURCHASED]->(:Order)-[:ORDERS]->(p:Product),
      (n:Customer)-[:PURCHASED]->(:Order)-[:ORDERS]->(reco:Product)
WHERE c.customerID = "SAVEA"
AND n.country = c.country
AND p.productID <> reco.productID
RETURN reco.productName
