# Getting started with Neo4j #

Adding a Node **Correctly**

    match (n:ToyNode {name:'Julian'})
    merge (n)-[:ToyRelation {relationship: 'fiancee'}]->(m:ToyNode {name:'Joyce', job:'store clerk'})

Adding a Node **Incorrectly**

    create (n:ToyNode {name:'Julian'})-[:ToyRelation {relationship: 'fiancee'}]->(m:ToyNode {name:'Joyce', job:'store clerk'})

Correct the mistake by deleting the bad nodes and edge
    
    match (n:ToyNode {name:'Joyce'})-[r]-(m) delete n, r, m

Modify a Node’s Information

    match (n:ToyNode) where n.name = 'Harry' set n.job = 'drummer'
    match (n:ToyNode) where n.name = 'Harry' set n.job = n.job + ['lead guitarist']

## Importing data into Neo4j ##

One way to "clean the slate" in Neo4j before importing

    match (a)-[r]->() delete a,r
    match (a) delete a


Script to Import Data Set: roadNetwork.csv (simple road network)

    LOAD CSV WITH HEADERS FROM "file:///C:/pathToFile/roadNetwork.csv" AS line
    MERGE (n:MyNode {Name:line.Source})
    MERGE (m:MyNode {Name:line.Target})
    MERGE (n) -[:TO {dist:line.distance}]-> (m)
    

