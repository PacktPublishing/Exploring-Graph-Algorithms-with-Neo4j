/*
Find shortest path between two stations
in the Paris subway graph
*/
// museum
:param start => "Louvre-Rivoli"

// to shopping
:param end => "Grands Boulevards"

// without weightProperty
MATCH (startNode:Station {name: $start})
MATCH (endNode:Station {name: $end})
CALL algo.shortestPath.stream(startNode, endNode, '')
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId), cost

// with weightProperty
MATCH (startNode:Station {name: $start})
MATCH (endNode:Station {name: $end})
CALL algo.shortestPath.stream(startNode, endNode, 'time')
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId), cost


/*
Direction parameter
*/

:param start => 'Boulogne-Jean-Jaurès'

:param end => 'Charles Michels'

// default direction = "BOTH"
MATCH (start:Station {name: $start})
MATCH (end:Station {name: $end})
CALL algo.shortestPath.stream(start, end, 'time')
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId), cost

MATCH (start:Station {name: $start})
MATCH (end:Station {name: $end})
CALL algo.shortestPath.stream(start, end, 'time', {direction: "INCOMING"})
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId), cost

MATCH (start:Station {name: $start})
MATCH (end:Station {name: $end})
CALL algo.shortestPath.stream(start, end, 'time', {direction: "OUTGOING"})
YIELD nodeId, cost
RETURN algo.getNodeById(nodeId), cost
