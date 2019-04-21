/*
Closeness algorithm on the whole graph
*/

CALL algo.closeness.stream(null, null)
YIELD nodeId, centrality
RETURN algo.getNodeById(nodeId).pId AS node, centrality
ORDER BY centrality DESC
LIMIT 10

/*We can also use cypher projection if needed
(here it just does exactly the same as the previous query)
*/
CALL algo.closeness.stream(
"MATCH (n) return id(n) as id", 
"MATCH (n)-[r]-(m) RETURN id(n) as source, id(m) as target", {graph: "cypher"})
YIELD nodeId, centrality
RETURN algo.getNodeById(nodeId).pId AS node, centrality
ORDER BY centrality DESC
LIMIT 10

/*
Adding a few nodes and relationships
*/
MERGE (T1:Teacher {pId: 100})
MERGE (T2:Teacher {pId: 200})
MERGE (T3:Teacher {pId: 300})
MERGE (T1)-[:LINKED_TO]->(T2)
MERGE (T2)-[:LINKED_TO]->(T3)

/*Call again the closeness algorithm on the whole graph (ie including the newly created nodes)
*/
CALL algo.closeness.stream(null, null, {improved: true})
YIELD nodeId, centrality
RETURN algo.getNodeById(nodeId).pId AS node, centrality
ORDER BY centrality DESC
LIMIT 10

/*Improved closeness centrality:
*/
CALL algo.betweenness.stream(null, null)
YIELD nodeId, centrality
RETURN algo.getNodeById(nodeId).pId AS node, centrality
ORDER BY centrality DESC
LIMIT 10

