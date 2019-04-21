/*
Load the data
*/
LOAD CSV FROM "file:///zkc.graph" AS row
  FIELDTERMINATOR " "
  MERGE (p:Person {pId: toInteger(row[0]) + 1})
  MERGE (q:Person {pId: toInteger(row[1]) + 1})
  MERGE (p)-[:LINKED_TO]->(q)

/*
Count relationships
*/
MATCH (n)-[r]->() 
WITH n, COUNT(r) as c
RETURN n, c
ORDER BY c DESC

/*
Count relationships fraction
*/
MATCH (n)-[r]->() 
WITH count(r) as total
MATCH (n)-[r]-()
WITH n, COUNT(r)  / toFloat(total) as pctg
RETURN n.pId, round(pctg * 100)/100
ORDER BY pctg DESC

/*
Degree centrality algorithm
*/
CALL algo.degree.stream(null, null, {direction: "BOTH"})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId), score
ORDER BY score DESC

