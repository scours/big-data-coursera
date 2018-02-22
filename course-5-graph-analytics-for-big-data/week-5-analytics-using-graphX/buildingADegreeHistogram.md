# Building a degree histogram #
1. Count the number of vertices and edges
2. Define a min and max function for Spark’s reduce method
3. Compute min and max degrees
4. Compute the histogram data of the degree of connectedness

**File 2** 

## Count the number of vertices and edges ##
Print the number of nodes and edges
	
    metrosGraph.numVertices
    metrosGraph.numEdges

## Define a min and max function for Spark’s reduce method ##
Define a min and max function

    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 <= b._2) a else b
    }
        
    
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    

## Compute min and max degrees ##
Find which VertexId ( 5 in our example) and the edge count of the vertex with the most **OUT** edges. Then print the returned vertex

    metrosGraph.outDegrees.reduce(max)
    metrosGraph.vertices.filter(_._1 == 5).collect()

Find which VertexId ( 108 in our example) and the edge count of the vertex with the most **IN** edges. Then print the returned vertex

    metrosGraph.inDegrees.reduce(max)
	metrosGraph.vertices.filter(_._1 == 108).collect()

Find the number of vertices that have only one out edge

    metrosGraph.outDegrees.filter(_._2 <= 1).count

Find the maximum and minimum degrees of the connections in the network

    metrosGraph.degrees.reduce(max)
    metrosGraph.degrees.reduce(min)

## Compute the histogram data of the degree of connectedness ##
Print the histogram data of the degrees for countries only

    metrosGraph.degrees.
      filter { case (vid, count) => vid >= 100 }. // Apply filter so only VertexId < 100 (countries) are included
      map(t => (t._2,t._1)).
      groupByKey.map(t => (t._1,t._2.size)).
      sortBy(_._1).collect()
    


