// clear graph
MATCH (n)
DETACH DELETE n

// create small test graph
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

MATCH (n)
RETURN n

// to see the relationships once we changed the browser configuration
MATCH (n)-[r]-()
RETURN n, r

MATCH (A:Node {name: "A"})
MATCH (E:Node {name: "E"})
CALL algo.shortestPath.stream(A, E, "distance")
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId).name, cost
