# Joining graph datasets #
1. Create a new dataset
2. Join two datasets with JoinVertices
3. Join two datasets with outerJoinVertices
4. Create a new return type for the joined vertices

**File 5**


## Create a new dataset ##

Run Spark Shell

    spark-shell

Set log level to error in order to suppress the info and warn messages so the output is easier to read.
    
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

Import the GraphX and RDD libraries.

    import org.apache.spark.graphx._
    import org.apache.spark.rdd._

Define a simple list of vertices containing five international airports.

    val airports: RDD[(VertexId, String)] = sc.parallelize(
    List((1L, "Los Angeles International Airport"),
      (2L, "Narita International Airport"),
      (3L, "Singapore Changi Airport"),
      (4L, "Charles de Gaulle Airport"),
      (5L, "Toronto Pearson International Airport")))

### Define a list of edges that will make up the flights. ###
Two airports are connected in this graph if there is a flight between them. We will assign a made up flight number to each flight.

    val flights: RDD[Edge[String]] = sc.parallelize(
      List(Edge(1L,4L,"AA1123"),
    Edge(2L, 4L, "JL5427"),
    Edge(3L, 5L, "SQ9338"),
    Edge(1L, 5L, "AA6653"),
    Edge(3L, 4L, "SQ4521")))

Define the **flightGraph** graph from the airports vertices and the flights edges.

    val flightGraph = Graph(airports, flights)

##### Print the departing and arrival airport and the flight number for each triplet in the flightGraph graph. #####
Each triplet in the **flightGraph** graph represents a flight between two airports.

    flightGraph.triplets.foreach(t => println("Departs from: " + t.srcAttr + " - Arrives at: " + t.dstAttr + " - Flight Number: " + t.attr))

##### Define an AirportInformation class to store the airport city and code. #####
Let's define a dataset with airport information so we can join the airport information dataset with the datasets that we have already defined.

    case class AirportInformation(city: String, code: String)

##### Define the list of airport information vertices. #####
Note: We do not have airport information defined for each airport in **flightGraph** graph and we have airport information for airports not in **flightGraph** graph.

    val airportInformation: RDD[(VertexId, AirportInformation)] = sc.parallelize(
      List((2L, AirportInformation("Tokyo", "NRT")),
    (3L, AirportInformation("Singapore", "SIN")),
    (4L, AirportInformation("Paris", "CDG")),
    (5L, AirportInformation("Toronto", "YYZ")),
    (6L, AirportInformation("London", "LHR")),
    (7L, AirportInformation("Hong Kong", "HKG"))))

## Join two datasets with JoinVertices ##
In this first example we are going to use joinVertices to join the airport information **flightGraph** graph.

Create a mapping function that appends the city name to the name of the airport.

    def appendAirportInformation(id: VertexId, name: String, airportInformation: AirportInformation): String = name + ":"+ airportInformation.city

Use joinVertices on **flightGraph** to join the airportInformation vertices to a new graph called **flightJoinedGraph** using the appendAirportInformation mapping function.

    val flightJoinedGraph =  flightGraph.joinVertices(airportInformation)(appendAirportInformation)
    flightJoinedGraph.vertices.foreach(println)

## Join two datasets with outerJoinVertices ##
Use outerJoinVertices on **flightGraph** to join the airportInformation vertices with additional airportInformation such as city and code, to a new graph called **flightOuterJoinedGraph**. Using the => operator which is just syntactic sugar for creating instances of functions.

    val flightOuterJoinedGraph = flightGraph.outerJoinVertices(airportInformation)((_,name, airportInformation) => (name, airportInformation))
    flightOuterJoinedGraph.vertices.foreach(println)

Use outerJoinVertices on **flightGraph** to join the airportInformation vertices with additional airportInformation such as city and code, to a new graph called **flightOuterJoinedGraphTwo**, but this time printing **'NA'** if there is no additional information.

    val flightOuterJoinedGraphTwo = flightGraph.outerJoinVertices(airportInformation)((_, name, airportInformation) => (name, airportInformation.getOrElse(AirportInformation("NA","NA"))))
    flightOuterJoinedGraphTwo.vertices.foreach(println)

## Create a new return type for the joined vertices ##
Create a case class called **Airport** to store the information for the name, city, and code of the airport.

    case class Airport(name: String, city: String, code: String)

Print the airportInformation with the name, city, and code within each other.

      val flightOuterJoinedGraphThree = flightGraph.outerJoinVertices(airportInformation)((_, name, b) => b match {
      case Some(airportInformation) => Airport(name, airportInformation.city, airportInformation.code)
      case None => Airport(name, "", "")
    })
    flightOuterJoinedGraphThree.vertices.foreach(println)



