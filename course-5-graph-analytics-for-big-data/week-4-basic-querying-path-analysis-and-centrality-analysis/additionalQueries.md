# Additional queries #

Count the number of shortest paths there are between the node named **‘BRCA1’** and the node named **‘NBR1’**

    MATCH p = allShortestPaths((source)-[r:ASSOCIATED_TO*]-(destination))
    WHERE source.Name='BRCA1' AND destination.Name = 'NBR1'
    RETURN count(EXTRACT(n IN NODES(p)| n.Name))

Find the top 2 nodes with the highest outdegree

    match (n:SymbolA)-[r]->()
    return n.Name as Node, count(r) as Outdegree
    order by Outdegree desc limit 2
    union
    match (a:SymbolA)-[r]->(leaf)
    where not((leaf)-->())
    return leaf.Name as Node, 0 as Outdegree limit 2
    

Create a degree histogram for the network, then calculate how many nodes are in the graph having a degree of 3

    match (n:SymbolA)-[r]-()
    with n as nodes, count(distinct r) as degree
    return degree, count(nodes) order by degree asc
