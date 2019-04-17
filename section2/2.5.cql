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
Find k=5 shortest paths from A to E (streaming)
*/
MATCH (A:Node {name: "A"})
MATCH (E:Node {name: "E"})
CALL algo.kShortestPaths.stream(A, E, 5, "distance")
YIELD index, nodeIds, costs
RETURN index, nodeIds,costs

// improve rendering by using getNodesById (notice the s!)
MATCH (A:Node {name: "A"})
MATCH (E:Node {name: "E"})
CALL algo.kShortestPaths.stream(A, E, 5, "distance")
YIELD index, nodeIds, costs
RETURN index,
       [node in algo.getNodesById(nodeIds[1..-1]) | node.name] AS nodes,
       costs

/*
path parameter
*/
MATCH (A:Node {name: "A"})
MATCH (E:Node {name: "E"})
CALL algo.kShortestPaths.stream(A, E, 1, 'distance', {path: true})
YIELD path
RETURN path

// => creates NEXT relationship

/*
Writing the result back to the graph
*/
MATCH (A:Node {name: "A"})
MATCH (E:Node {name: "E"})
CALL algo.kShortestPaths(A, E, 3, 'distance' ,{})
YIELD resultCount
RETURN resultCount

// see the path as new relationships!
MATCH p=(A:Node {name: "A"})-[r:PATH_0|PATH_1|PATH_2*]->(E:Node {name: "E"}) 
RETURN p
