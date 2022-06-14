package org.gbif.pipelines.events
import org.apache.log4j.{Level, Logger}
import org.apache.logging.log4j.util.Strings
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.gbif.pipelines.io.avro.{DenormalisedEvent}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
 * Pipeline that uses spark's graphx API to construct the parent hierarchy
 * for events in a dataset.
 *
 * This pipeline produces an AVRO export that can then be used in indexing
 * with the outputs of the interpretation pipeline to populate geospatial
 * and temporal fields that are otherwise missing from events but are present on parent events.
 */
object DenormalisationPipeline {

  val KEY_FIELDS = Array(
    "id",
    "event_type",
    "location_id",
    "latitude",
    "longitude",
    "state_province",
    "country_code",
    "year",
    "month"
  )

  val FIELD_DELIM = "$$$"
  val FIELD_DELIM_ESCAPED = "\\$\\$\\$"
  val PATH_DELIM = "|||"
  val PATH_DELIM_ESCAPED = "\\|\\|\\|"

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

    val schemaAvro = DenormalisedEvent.getClassSchema.toString(true)

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
    val eventCoreDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/event_core/*.avro").as("event")

    System.out.println("Load location")
    val locationDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/location/*.avro").as("location")

    System.out.println("Load temporal")
    val temporalDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/temporal/*.avro").as("temporal")

    System.out.println("Join")
    val joined_df = eventCoreDF.
      join(locationDF, col("event.id") === col("location.id"), "inner").
      join(temporalDF, col("event.id") === col("temporal.id"), "inner")

    System.out.println("select from join")
    val eventsDF = joined_df.filter("event.id is NOT null").select(
      col("event.id").as("id"),
      col("eventType.concept").as("event_type"),
      col("parentEventID").as("parent_event_id"),
      coalesce(col("locationID"), lit("0")).as("location_id"),
      coalesce(col("decimalLatitude"), lit("0")).as("latitude"),
      coalesce(col("decimalLongitude"), lit("0")).as("longitude"),
      coalesce(col("year"), lit("0")).as("year"),
      coalesce(col("month"), lit("0")).as("month"),
      coalesce(col("stateProvince"), lit("0")).as("state_province"),
      coalesce(col("countryCode"), lit("0")).as("country_code")
    )

    // primary key, root, path - dataframe to graphx for vertices
    val verticesDF = eventsDF.selectExpr("id",
      s"concat(${String.join(s", '${FIELD_DELIM}', ", KEY_FIELDS:_*)})",
      s"concat(${String.join(s", '${FIELD_DELIM}', ", KEY_FIELDS:_*)})"
    )

    // parent to child - dataframe to graphx for edges
    val edgesDF = eventsDF.selectExpr("parent_event_id","id").filter("parent_event_id is not null")

    // call the function
    val hierarchyDF = calcTopLevelHierarchy(verticesDF, edgesDF)
      .map {
        case (pk, (level, root, path, iscyclic, isleaf)) =>
          (pk.asInstanceOf[String], level, root.asInstanceOf[String], path, iscyclic, isleaf)
      }
      .toDF("id_pk", "level", "root", "path", "iscyclic", "isleaf").cache()

    import spark.implicits._

    // extend original table with new columns
    val eventHierarchy = hierarchyDF
      .join(eventsDF, eventsDF.col("id") === hierarchyDF.col("id_pk"))
      .selectExpr("id", "path")
      .filter("id is not NULL")
      .filter("path is not NULL")

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[DenormalisedEvent]
    val sqlType = SchemaConverters.toSqlType(DenormalisedEvent.getClassSchema)
    val rowRDD = eventHierarchy.rdd.map(row => genericRecordToRow(row, sqlType.dataType.asInstanceOf[StructType]))
    val df = spark.sqlContext.createDataFrame(rowRDD , sqlType.dataType.asInstanceOf[StructType])

    df.write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .options(Map("avroSchema" -> schemaAvro.toString))
      .save(s"${hdfsPath}/${datasetId}/${attempt}/event/event_hierarchy/")
  }

  def genericRecordToRow(row:Row, sqlType:StructType): Row = {
      val eventID = row.getString(0)
      val path = row.getString(1)
      val pathElements = path.split(PATH_DELIM_ESCAPED).filter(pathElement => Strings.isNotBlank(pathElement))
      val parents:Array[Row] = pathElements.map(pathElement => {
        val elem = pathElement.split(FIELD_DELIM_ESCAPED)
        // this needs to match the order of the AVRO schema
        if (elem !=null && elem.length ==9){
          Row(
            elem(0), // eventID
            elem(1), // eventType
            if (elem(2) != null && elem(2) != "0") elem(2) else null, // locationID
            if (elem(3) != null && elem(3) != "0") elem(3).toDouble else null, // lat
            if (elem(4) != null && elem(4) != "0") elem(4).toDouble else null,  // lon
            if (elem(5) != null && elem(5) != "0") elem(5) else null, // stateProvince
            if (elem(6) != null && elem(6) != "0") elem(6) else null, // countryCode
            if (elem(7) != null && elem(7) != "0") elem(7).toInt else null,   // year
            if (elem(8) != null && elem(8) != "0") elem(8).toInt else null    // month
          )
        } else {
          null
        }
    }).filter(_ != null).toArray
    new GenericRowWithSchema(Array(eventID, parents), sqlType)
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
        ( ExtendedMurmurHash.hash(x._1.toString),
            (x._1, x._2, x._3.asInstanceOf[String])
        )
      }

    // create the edge RDD
    // top down relationship
    val EdgesRDD = edgeDF
      .rdd
      .map { x => (x.get(0), x.get(1)) }
      .map { x => Edge(
        ExtendedMurmurHash.hash(x._1.toString),  // parent event ID
        ExtendedMurmurHash.hash(x._2.toString),  // event ID
        "topdown")
      }

    // create graph
    val graph = Graph(verticesRDD, EdgesRDD).cache()

    // add more dummy attributes to the vertices - id, level, root, path, isCyclic, existing value of current vertex to build path, isleaf, pk
    val initialGraph = graph.mapVertices((id, vertex) => {
      if (vertex != null){
        (
          id,
          0,
          vertex._2,
          List(vertex._3),
          0,
          vertex._3,
          1,
          vertex._1)
      } else {
        null
      }
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
        if (v != null) {
          (v._8, (v._2, v._3, PATH_DELIM + v._4.reverse.mkString(PATH_DELIM), v._5, v._7))
        } else {
          null
        }
      }
    }.filter(_ != null)

    hierarchyOutRDD
  }

  //Change the value of the vertex
  def setMsg(vertexId: VertexId, value: (Long,Int,Any,List[String], Int,String,Int,Any),
             message: (Long,Int, Any,List[String],Int,Int)): (Long,Int, Any,List[String],Int,String,Int,Any) = {

    if (value == null)
      return null

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

    if (sourceVertex == null || destinationVertex == null)
      return Iterator.empty

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
      if (sourceVertex._7 == 1){ //is NOT leaf
        Iterator((triplet.srcId, (sourceVertex._1,sourceVertex._2,sourceVertex._3, sourceVertex._4 ,0, 0 )))
      } else { // set new values
        Iterator((triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4, 0, 1)))
      }
    }
  }

  // Receive values from all connected vertices
  def mergeMsg(msg1: (Long,Int,Any,List[String],Int,Int), msg2: (Long,Int, Any,List[String],Int,Int)): (Long,Int,Any,List[String],Int,Int) = msg2
}

object ExtendedMurmurHash {
  val seed = 0xf7ca7fd2

  def hash(u: String): Long = {
    val a = scala.util.hashing.MurmurHash3.stringHash(u, seed)
    val b = scala.util.hashing.MurmurHash3.stringHash(u.reverse.toString, seed)
    // shift a 32 bits to the left, leaving 32 lower bits zeroed
    // mask b with "1111...1" (32 bits)
    // bitwise "OR" between both leaving a 64 bit hash of the string using Murmur32 on each portion
    val c: Long = a.toLong << 32 | (b & 0xffffffffL)
    c
  }
}