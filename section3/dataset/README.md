# The Zachary's karate club dataset

It will be used in this section and throughout the rest of this course.

To import the data:

    LOAD CSV FROM "file:///zkc.graph" AS row
      FIELDTERMINATOR " "
      MERGE (p:Person {pId: toInteger(row[0]) + 1})
      MERGE (q:Person {pId: toInteger(row[1]) + 1})
      MERGE (p)-[:LINKED_TO]->(q)
