/*
Extract data for link prediction
*/
MATCH (n1)
MATCH (n2)
WHERE n1.id < n2.id
OPTIONAL MATCH (n1)-[r]-(n2)
RETURN n1.id as p1, n2.id as p2, 
       r IS NOT NULL as label



/*
Extract data for link prediction with some "graphy" features:
*/
MATCH (n1)
MATCH (n2)
WHERE n1.id < n2.id
OPTIONAL MATCH (n1)-[r]-(n2)
RETURN n1.id as p1, n2.id as p2,
       algo.similarity.jaccard(n1, n2) as jac,
       algo.linkprediction.commonNeighbors(n1, n2) as cn,
       algo.linkprediction.adamicAdar(n1, n2) as aa,
       r IS NOT NULL as label
