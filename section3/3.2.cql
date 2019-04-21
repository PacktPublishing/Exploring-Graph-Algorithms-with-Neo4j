/*
Page rank algorithm on all nodes
Default damping factor: 0.85
*/
CALL algo.pageRank.stream(null, null, {direction: "BOTH"})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).id, score
ORDER BY score DESC

/*
Page rank with damping factor = 0.5
*/
CALL algo.pageRank.stream(null, null, {direction: "BOTH", dampingFactor: 0.5})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).pId, score
ORDER BY score DESC

/*
Page rank with damping factor = 1
*/
CALL algo.pageRank.stream(null, null, {direction: "BOTH", dampingFactor: 1})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).pId, score
ORDER BY score DESC

/*
Changing the number of iterations for better convergence
*/
CALL algo.pageRank.stream(null, null, {direction: "BOTH", dampingFactor: 0.5, iterations: 50})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).pId, score
ORDER BY score DESC

/*
Using a weight property
*/

CALL algo.pageRank.stream(null, null, {direction: "BOTH", dampingFactor: 0.5, iterations: 50, weightProperty: "weight"})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).pId, score
ORDER BY score DESC
