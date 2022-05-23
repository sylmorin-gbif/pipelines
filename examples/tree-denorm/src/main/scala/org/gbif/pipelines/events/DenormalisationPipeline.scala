package org.gbif.pipelines.events
import org.apache.avro.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types.{Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.hashing.MurmurHash3

object DenormalisationPipeline {

  // Test with some sample data
  def main(args: Array[String]): Unit = {

    if (args.length < 3){
      println("Please supply <DatasetId> <HDFS_BasePath> <Attempt>")
      println("e.g. dr18391 hdfs://localhost:9000/pipelines-data 1")
      return;
    }

    val datasetId = args(0)
    val hdfsPath =  args(1)
    val attempt =  args(2)

    val localTest = if (args.length > 3) {
      args(3).toBoolean
    }  else {
      false
    }

    val schemaAvro = new Schema.Parser().parse(
        """{
          "name": "DenormalisedEvent",
          "namespace": "org.gbif.pipelines.io.avro",
          "type": "record",
          "fields": [
            {"name": "id", "type": ["null", "string"], "default" : null},
            {"name": "path", "type": ["null", "string"], "default" : null }
          ]
         }""")

    // Mask log
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = if (localTest) {
      SparkSession
        .builder
        .master("local[*]")
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()
    } else {
      SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()
    }

    // RDD to DF, implicit conversion
    import spark.implicits._

    System.out.println("Load events")
    val eventCoreDF = spark.read.format("avro").load(s"${hdfsPath}/${datasetId}/${attempt}/interpreted/event_core/*.avro").as("event")
    System.out.println("Load location")
    val locationDF = spark.read.format("avro").load(s"${hdfsPath}/${datasetId}/${attempt}/interpreted/location/*.avro").as("location")
    System.out.println("Load temporal")
    val temporalDF = spark.read.format("avro").load(s"${hdfsPath}/${datasetId}/${attempt}/interpreted/temporal/*.avro").as("temporal")

    System.out.println("Join")
    val joined_df = eventCoreDF.
      join(locationDF, col("event.id") === col("location.id"), "inner").
      join(temporalDF, col("event.id") === col("temporal.id"), "inner")

    System.out.println("select from join")
    val eventsDF = joined_df.select(
      col("event.id").as("id"),
      col("eventType.concept").as("event_type"),
      col("parentEventID").as("parent_event_id"),
      coalesce(col("decimalLatitude"), lit("0")).as("latitude"),
      coalesce(col("decimalLongitude"), lit("0")).as("longitude"),
      coalesce(col("year"), lit("0")).as("year")
    )

    // primary key, root, path - dataframe to graphx for vertices
    val verticiesDF = eventsDF.selectExpr("id",
      "concat(id, '$$$', event_type, '$$$', latitude, '$$$', longitude, '$$$', year)",
      "concat(id, '$$$', event_type, '$$$', latitude, '$$$', longitude, '$$$', year)"
    )

    // parent to child - dataframe to graphx for edges
    val edgesDF = eventsDF.selectExpr("parent_event_id","id").filter("parent_event_id is not null")

    // call the function
    val hierarchyDF = calcTopLevelHierarchy(verticiesDF, edgesDF)
      .map {
        case (pk, (level, root, path, iscyclic, isleaf)) =>
          (pk.asInstanceOf[String], level, root.asInstanceOf[String], path, iscyclic, isleaf)
      }
      .toDF("id_pk", "level", "root", "path", "iscyclic", "isleaf").cache()

    // extend original table with new columns
    val eventHierarchy = hierarchyDF
      .join(eventsDF, eventsDF.col("id") === hierarchyDF.col("id_pk"))
      .selectExpr("id", "path")
      .filter("id is not NULL")
      .filter("path is not NULL").toDF()

    // get schema
    val schema = eventHierarchy.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if c.equals("id") || c.equals("path") => StructField( c, t, nullable = false, m)
      case y: StructField => y
    })

    // apply new schema
    eventHierarchy.sqlContext.createDataFrame( eventHierarchy.rdd, newSchema )

    // print
    eventHierarchy.write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .options(Map("avroSchema" -> schemaAvro.toString))
      .save(s"${hdfsPath}/${datasetId}/${attempt}/interpreted/event_hierarchy/")
  }

  // The code below demonstrates use of Graphx Pregel API - Scala 2.11+
  // functions to build the top down hierarchy

  //setup & call the pregel api
  //Set up and call the pregel api
  def calcTopLevelHierarchy(vertexDF: DataFrame, edgeDF: DataFrame): RDD[(Any,(Int,Any,String,Int,Int))] = {

    // create the vertex RDD
    // primary key, root, path
    val verticesRDD = vertexDF
      .rdd
      .map { x => (x.get(0), x.get(1), x.get(2)) }
      .map { x =>
        ( MurmurHash3.stringHash(x._1.toString).toLong,
            (x._1, x._2, x._3.asInstanceOf[String])
        )
      }

    // create the edge RDD
    // top down relationship
    val EdgesRDD = edgeDF
      .rdd
      .map { x => (x.get(0), x.get(1)) }
      .map { x => Edge(
        MurmurHash3.stringHash(x._1.toString).toLong,  // parent event ID
        MurmurHash3.stringHash(x._2.toString).toLong,  // event ID
        "topdown")
      }

    // create graph
    val graph = Graph(verticesRDD, EdgesRDD).cache()

    val pathSeparator = """|||"""

    // add more dummy attributes to the vertices - id, level, root, path, isCyclic, existing value of current vertex to build path, isleaf, pk
    val initialGraph = graph.mapVertices((id, vertex) => {
      (
        id,
        0,
        vertex._2,
        List(vertex._3),
        0,
        vertex._3,
        1,
        vertex._1)
    })

    // Initialization message
    val initialMsg = (0L, 0, 0.asInstanceOf[Any], List("dummy"), 0, 1)

    val hierarchyRDD = initialGraph.pregel(
      initialMsg,
      Int.MaxValue, // The number of iterations, set to the current value, indicates that the iteration will continue indefinitely
      EdgeDirection.Out)(
      setMsg,
      sendMsg,
      mergeMsg)

    // build the path from the list
    val hierarchyOutRDD = hierarchyRDD.vertices.map {
      case (id, v) => {
        (v._8, (v._2, v._3, pathSeparator + v._4.reverse.mkString(pathSeparator), v._5, v._7 ))
      }
    }

    hierarchyOutRDD
  }

  //Change the value of the vertex
  def setMsg(vertexId: VertexId, value: (Long,Int,Any,List[String], Int,String,Int,Any),
             message: (Long,Int, Any,List[String],Int,Int)): (Long,Int, Any,List[String],Int,String,Int,Any) = {


    if (message._2 < 1) { //superstep 0 - initialize
      (value._1,value._2+1,value._3,value._4,value._5,value._6,value._7,value._8)
    }
    else if ( message._5 == 1) { // set isCyclic (judge whether it is a ring)
      (value._1, value._2, value._3, value._4, message._5, value._6, value._7,value._8)
    } else if ( message._6 == 0 ) { // set isleaf
      (value._1, value._2, value._3, value._4, value._5, value._6, message._6,value._8)
    }
    else {
      // set new values
      ( message._1,value._2+1, message._3, value._6 :: message._4 , value._5,value._6,value._7,value._8)
    }
  }

  // Send values to vertices
  def sendMsg(triplet: EdgeTriplet[(Long,Int,Any,List[String],Int,String,Int,Any), _]): Iterator[(VertexId, (Long,Int,Any,List[String],Int,Int))] = {

    val sourceVertex = triplet.srcAttr
    val destinationVertex = triplet.dstAttr

    // Check whether it is a dead ring, that is, a is the leader of b and b is the leader of A
    // check for icyclic
    if (sourceVertex._1 == triplet.dstId || sourceVertex._1 == destinationVertex._1) {
      if (destinationVertex._5 == 0) { //set iscyclic
        Iterator((triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4, 1, sourceVertex._7)))
      } else {
        Iterator.empty
      }

    } else {

      // Judge whether it is a leaf node or a node without child nodes.
      // It belongs to a leaf node and the root node does not count
      if (sourceVertex._7 == 1) //is NOT leaf
      {
        Iterator((triplet.srcId, (sourceVertex._1,sourceVertex._2,sourceVertex._3, sourceVertex._4 ,0, 0 )))
      }
      else { // set new values
        Iterator((triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4, 0, 1)))
      }
    }
  }

  // Receive values from all connected vertices
  def mergeMsg(msg1: (Long,Int,Any,List[String],Int,Int), msg2: (Long,Int, Any,List[String],Int,Int)): (Long,Int,Any,List[String],Int,Int) = msg2
}