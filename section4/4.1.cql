/* Here, we are using the Karate Club dataset with the modifications
we introduced in previous section (ie the creation of three Teacher nodes)
*/

/*
Triangle count and clustering coefficient
*/
CALL algo.triangleCount.stream('Person', null)
YIELD nodeId, triangles, coefficient
RETURN algo.getNodeById(nodeId).pId AS person, triangles, coefficient
ORDER BY triangles DESC

/*
Order by coefficient
*/
CALL algo.triangleCount.stream('Person', null)
YIELD nodeId, triangles, coefficient
RETURN algo.getNodeById(nodeId).pId AS person, triangles, coefficient
ORDER BY coefficient DESC

/*
Connected components
*/
CALL algo.unionFind.stream("Person", null)
YIELD nodeId, setId
RETURN algo.getNodeById(nodeId).pId, setId
ORDER BY setId

/*
Connected  components on the whole graph (including Teachers)
*/
CALL algo.unionFind.stream(null, null)
YIELD nodeId, setId
RETURN algo.getNodeById(nodeId).pId, setId
ORDER BY setId DESC

/*
Label propagation
*/
CALL algo.labelPropagation.stream("Person", null, {direction: "BOTH"})
YIELD nodeId, label
RETURN algo.getNodeById(nodeId).pId, label
ORDER BY label

/*
Louvain method
*/
CALL algo.louvain.stream("Person", null)
YIELD nodeId, community
RETURN algo.getNodeById(nodeId).pId, community
ORDER BY community

