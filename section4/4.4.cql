/*
Compute similarity between two customers based on their purchased products
*/

// First, we need to find the list of product for each customer.
MATCH (c:Customer)-[:PURCHASED]->(:Order)-[:ORDERS]->(p:Product)
// In order to use the similarity algorithm procedure, we need to shape the data in a specific format:
WITH {item:id(c), categories: collect(id(p))} as customerData
WITH collect(customerData) as data
CALL algo.similarity.jaccard.stream(data, {similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, intersection, similarity
// let's see the result for a specific customer
WITH algo.getNodeById(item1) AS customer1, algo.getNodeById(item2) AS customer2, intersection, similarity
WHERE customer1.customerID = "QUEEN"
RETURN customer2.customerID, intersection, similarity
ORDER BY similarity DESC
LIMIT 10


// Use this to get the products purchassed by similar customers
MATCH (c:Customer)-[:PURCHASED]->(:Order)-[:ORDERS]->(p:Product)
WITH {item:id(c), categories: collect(id(p))} as customerData
WITH collect(customerData) as data
CALL algo.similarity.jaccard.stream(data, {similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, intersection, similarity
WITH algo.getNodeById(item1) AS customer1, algo.getNodeById(item2) AS customer2, intersection, similarity
WHERE customer1.customerID = "QUEEN"
WITH customer1, customer2, intersection, similarity // CHANGE THIS!
ORDER BY similarity DESC
LIMIT 10
// LINES TO BE ADDED
MATCH (customer2)-[:PURCHASED]->()-[:ORDERS]->(p2:Product)
MATCH (customer1)-[:PURCHASED]->()-[:ORDERS]->(p1:Product)
WHERE p1 <> p2
RETURN DISTINCT p2.productName

// Finally, order the recommendations
MATCH (c:Customer)-[:PURCHASED]->(:Order)-[:ORDERS]->(p:Product)
WITH {item:id(c), categories: collect(id(p))} as customerData
WITH collect(customerData) as data
CALL algo.similarity.jaccard.stream(data, {similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, intersection, similarity
WITH algo.getNodeById(item1) AS customer1, algo.getNodeById(item2) AS customer2, intersection, similarity
WHERE customer1.customerID = "QUEEN"
WITH customer1, customer2, intersection, similarity
ORDER BY similarity DESC
LIMIT 10
MATCH (customer2)-[:PURCHASED]->()-[:ORDERS]->(p2:Product)
MATCH (customer1)-[:PURCHASED]->()-[:ORDERS]->(p1:Product)
WHERE p1 <> p2
// LINES TO BE ADDED
WITH p2.productName as recoProductName, COUNT(DISTINCT customer2) as countSimilarCustomer
RETURN recoProductName, countSimilarCustomer
ORDER BY countSimilarCustomer DESC


/*
Cosine similarity between customers
*/

// First, create some ratings (randomly for testing purposes)
MATCH (c:Customer)-[:PURCHASED]->(:Order)-[:ORDERS]->(p:Product)
MERGE (c)-[:RATED {score: toInteger(round(rand() * 10))}]->(p)

// Then compute cosine similarity between 2 customers
MATCH (p1:Customer {customerID: "QUEEN"})-[score1:RATED]->(product)
MATCH (p2:Customer {customerID: "ROMEY"})-[score2:RATED]->(product)
RETURN p1.customerID as c1,
       p2.customerID as c2,
       algo.similarity.cosine(collect(score1.score), collect(score2.score)) AS similarity
