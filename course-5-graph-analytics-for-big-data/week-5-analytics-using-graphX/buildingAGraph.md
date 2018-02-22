# Building a graph  #
1. Start the Cloudera VM and Upload the datasets to HDFS
2. Import the GraphX libraries
3. Import the Vertices
4. Import the Edges
5. Create a Graph
6. Use Spark’s filter method to return Vertices in the graph 

**File 1**

## Extract and upload datasets into HDFS ##
    unzip ExamplesOfAnalytics.zip
    cd ExamplesOfAnalytics
    hdfs dfs -put EOADATA


## Start Spark Shell and libraries ##
    spark-shell --jars lib/gs-core-1.2.jar,lib/gs-ui-1.2.jar,lib/jcommon-1.0.16.jar,lib/jfreechart-1.0.13.jar,lib/breeze_2.10-0.9.jar,lib/breeze-viz_2.10-0.9.jar,lib/pherd-1.0.jar

## Import the GraphX libraries ##
Set log level to error, suppress info and warn messages.

    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
Import the Spark's GraphX and RDD libraries along with Scala's source library.

    import org.apache.spark.graphx._
    import org.apache.spark.rdd._
    
    import scala.io.Source

## Import the Vertices ##
Before importing any datasets, let's view what the files contain. Print the first 5 lines of each comma delimited text file.

    Source.fromFile("./EOADATA/metro.csv").getLines().take(5).foreach(println)
    Source.fromFile("./EOADATA/country.csv").getLines().take(5).foreach(println)
    Source.fromFile("./EOADATA/metro_country.csv").getLines().take(5).foreach(println)

Create case classes for the places (metros and countries).

    class PlaceNode(val name: String) extends Serializable
    case class Metro(override val name: String, population: Int) extends PlaceNode(name)
    case class Country(override val name: String) extends PlaceNode(name)

Read the comma delimited text file **metros.csv** into an RDD of Metro vertices, ignore lines that start with # (first line with headers) and map the columns to: id, Metro(name, population).

    val metros: RDD[(VertexId, PlaceNode)] =
      sc.textFile("./EOADATA/metro.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      (0L + row(0).toInt, Metro(row(1), row(2).toInt))
    }

Read the comma delimited text file country.csv into an RDD of Country vertices, ignore lines that start with # and map the columns to: id, Country(name). Add 100 to the country indexes so they are unique from the metro indexes.

    val countries: RDD[(VertexId, PlaceNode)] =
      sc.textFile("./EOADATA/country.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      (100L + row(0).toInt, Country(row(1)))
    }

## Import the Edges ##
Read the comma delimited text file **metro\_country.csv** into an RDD[Edge[Int]] collection. Add 100 to the countries' vertex id.

    val mclinks: RDD[Edge[Int]] =
      sc.textFile("./EOADATA/metro_country.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      Edge(0L + row(0).toInt, 100L + row(1).toInt, 1)
    }
    

## Create a Graph ##
Concatenate the two sets of nodes into a single RDD.


    val nodes = metros ++ countries

Pass the concatenated RDD to the Graph() factory method along with the RDD link.


    val metrosGraph = Graph(nodes, mclinks)

Print the first 5 vertices and edges.

    metrosGraph.vertices.take(5)
    metrosGraph.edges.take(5)

## Use Spark’s filter method to return Vertices in the graph ##
Filter all of the edges in metrosGraph that have a source vertex Id of 1 and create a map of destination vertex Ids.

    metrosGraph.edges.filter(_.srcId == 1).map(_.dstId).collect()

Similarly, filter all of the edges in metrosGraph where the destination vertexId is 103 and create a map of all of the source Ids.


    metrosGraph.edges.filter(_.dstId == 103).map(_.srcId).collect()
