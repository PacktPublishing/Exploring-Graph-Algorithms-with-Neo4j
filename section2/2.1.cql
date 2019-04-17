/*
 Empty db
*/
MATCH (n)
DETACH DELETE n

/*
Create test graph
*/
MERGE (A:City {name: "A"})
MERGE (B:City {name: "B"})
MERGE (C:City {name: "C"})
MERGE (D:City {name: "D"})
MERGE (E:City {name: "E"})
MERGE (A)-[:ROAD {closed: false}]->(B)
MERGE (B)-[:ROAD {closed: false}]->(C)
MERGE (C)-[:ROAD {closed: false}]->(D)
MERGE (D)-[:ROAD {closed: false}]->(E)
MERGE (B)-[:ROAD {closed: true}]->(E)
MERGE (A)-[:TRAIN {closed: false}]->(E)

// check the graph content
MATCH (n)
RETURN n

/*
First algorithm!
*/
// Select start/end nodes
MATCH (A:City {name: "A"})
MATCH (E:City {name: "E"})
// CALL the algorithm
CALL algo.shortestPath.stream(A, E, null)
// returns ordered list of node IDs for going from A to E, with cost
YIELD nodeId, cost
// Return this
RETURN nodeId, cost


// getNodeById
RETURN algo.getNodeById(0)


MATCH (A:City {name: "A"})
MATCH (E:City {name: "E"})
CALL algo.shortestPath.stream(A, E, null)
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId), cost

MATCH (A:City {name: "A"})
MATCH (E:City {name: "E"})
CALL algo.shortestPath.stream(A, E, null)
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId).name, cost


/*
Using a subgraph
*/
// Only using ROAD type relationships
MATCH (A:City {name: "A"})
MATCH (E:City {name: "E"})
CALL algo.shortestPath.stream(A, E, null, {relationshipQuery: "ROAD"})
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId).name, cost

/*
Even more control with cypher projections
*/
MATCH (A:City {name: "A"})
MATCH (E:City {name: "E"})
CALL algo.shortestPath.stream(A, E, "", {
			nodeQuery: 'MATCH (n:City) RETURN id(n) as id', 
                        relationshipQuery: 'MATCH (n)-[r:ROAD]->(m) WHERE r.closed = false RETURN id(n) as source, id(m) as target',
                        graph: "cypher"
                   }
)
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId).name, cost


/*
Writing back results to the graph
*/
// 1. Calling the algorithm
MATCH (A:City {name: "A"})
MATCH (E:City {name: "E"})
CALL algo.shortestPath(A, E, null, {writeProperty: "cost"})
YIELD nodeCount, totalCost, loadMillis, evalMillis, writeMillis
RETURN nodeCount, totalCost, loadMillis, evalMillis, writeMillis

//2. Watch the results
MATCH (n:City)
WHERE n.cost IS NOT NULL
RETURN n


// All of the above about cypher projection is still valid
MATCH (A:City {name: "A"})
MATCH (E:City {name: "E"})
CALL algo.shortestPath(A, E, "", {
                        writeProperty: "cost2",
			nodeQuery: 'MATCH (n:City) RETURN id(n) as id', 
                        relationshipQuery: 'MATCH (n)-[r:ROAD]->(m) WHERE r.closed = false RETURN id(n) as source, id(m) as target',
                        graph: "cypher"
                   }
)
YIELD nodeCount, totalCost, loadMillis, evalMillis, writeMillis
RETURN nodeCount, totalCost, loadMillis, evalMillis, writeMillis

MATCH (n:City)
WHERE n.cost2 IS NOT NULL
RETURN n

/*
 Select only relationships for display
*/
MATCH (n:City)
WHERE n.cost2 IS NOT NULL
WITH n
ORDER BY n.cost2
WITH collect(n) as path
UNWIND range(0, size(path)-1) AS index
WITH path[index] AS currentNode, path[index+1] AS nextNode
MATCH (currentNode)-[r:ROAD]->(nextNode)
WHERE r.closed = false
RETURN currentNode, r, nextNode
