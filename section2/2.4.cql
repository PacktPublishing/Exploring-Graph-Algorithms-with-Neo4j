/*
Create test graph
*/
WITH [	
    // start, end, distance
	["A", "B", 10],
      	["A", "C", 33],
	["A", "D", 35],
	["C", "D", 28],
	["B", "C", 20],
	["C", "E", 6],
	["D", "E", 40]
] AS nested
UNWIND nested AS row
MERGE (n:Node {name: row[0]})
MERGE (m:Node {name: row[1]})
MERGE (n)-[r:LINK {distance: row[2]}]->(m)

/*
Find shortest paths between A and all other nodes
*/
MATCH (A:Node {name:"A"})
CALL algo.shortestPath.deltaStepping.stream(A, "distance", 1.0)
YIELD nodeId, distance
RETURN algo.getNodeById(nodeId).name AS end, distance
ORDER BY distance

/*
What happens if there is no path from A to one of the nodes?
*/
MERGE (:Node {name: "F"})

MATCH (A:Node {name:"A"})
CALL algo.shortestPath.deltaStepping.stream(A, "distance", 1.0)
YIELD nodeId, distance
RETURN algo.getNodeById(nodeId).name AS end, distance
ORDER BY distance

// GAL contains helper function to filter thos nodes out
MATCH (n:Node {name:"A"})
CALL algo.shortestPath.deltaStepping.stream(n, "distance", 10.0, {relationshipQuery:"LINK"})
YIELD nodeId, distance
WHERE algo.isFinite(distance)
RETURN algo.getNodeById(nodeId).name AS end, distance
ORDER BY distance
