/*
Common neighbours for two nodes:
*/
MATCH (p1:Person {pId: 24})
MATCH (p2:Person {pId: 25})
RETURN algo.linkprediction.commonNeighbors(p1, p2) AS score

/*
Nodes with the highest number of common neighbours
*/
MATCH (p1:Person)
MATCH (p2:Person)
WHERE p1.pId < p2.pId AND NOT (p1)-[:LINKED_TO]-(p2)
WITH p1.pId as p1, p2.pId as p2,  algo.linkprediction.commonNeighbors(p1, p2) AS score
RETURN p1, p2, score
ORDER BY score DESC
LIMIT 10


/*
same community function (helper to extract feature)

First, create community property
*/
CALL algo.louvain(null, null,
  {write:true, writeProperty:'louv'})
YIELD nodes, communityCount;

/*
Then use linkprediction.sameCommunity function
*/
MATCH (p1:Person {pId: 3})
MATCH (p2:Person {pId: 34})
RETURN algo.linkprediction.sameCommunity(p1, p2, "louv") AS score
