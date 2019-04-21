/*
Download dataset from

https://snap.stanford.edu/data/email-Eu-core.html

Unzip:

gunzip email-Eu-core.txt.gz
gunzip email-Eu-core-department-labels.txt.gz 

Load data into neo4j:
*/

USING PERIODIC COMMIT
LOAD CSV FROM "file:///email-Eu-core.txt" AS line
FIELDTERMINATOR ' '
WITH line
MERGE (p:Person {pId: toInteger(line[0])})
MERGE (p2:Person {pId: toInteger(line[1])})
MERGE (p)-[:WROTE_TO]-(p2)
;


/*
Also save department
*/
USING PERIODIC COMMIT
LOAD CSV FROM "file:///email-Eu-core-department-labels.txt" AS line
FIELDTERMINATOR ' '
WITH line
MERGE (p:Person {pId: toInteger(line[0])})
SET p.dep = toInteger(line[1])
 ;


/*
betweenness centrality
*/
CALL algo.betweenness(null, null, {direction: "BOTH", writeProperty: "bwn"})
YIELD nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis;

/*
Discover the results
*/
MATCH (n)
WHERE n.dep = 38 OR n.dep = 2 OR n.dep = 20
RETURN n.pId, n.bwn
ORDER BY n.bwn DESC
LIMIT 10
