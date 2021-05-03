package com.schiphol.kiara.assignment

import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import com.schiphol.kiara.assignment.SparkSessionWrapper
import org.apache.spark.sql.catalyst.ScalaReflection
import shared._

// Use Spark structured streaming to change your job into a streaming job,
// and use the dataset file as a source.
object streaming extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    // delete output directory if exists
    val outPath = "./data/out/stream-top"
    new Directory(new File(outPath)).deleteRecursively()

    val query = readRoutesStream()
      .transform(aggregateStream)
    writeRouteStream(query, outPath)
    // sorting needs complete mode, which we can use for testing but not to write to csv
    // print top 10 airports used as source airport
    printStream(query.sort(col("count").desc).limit(10))
  }

  // read stream
  def readRoutesStream(): Dataset[FlightRouteRaw] = {
    import spark.implicits._
    spark
      .readStream
      .option("rowsPerSecond", 1) // use 1 row per micro batch because we do not have much data
      .schema(ScalaReflection.schemaFor[FlightRouteRaw].dataType.asInstanceOf[StructType])
      .option("header", "false")
      .csv("./data/raw")
      .as[FlightRouteRaw]
  }

  def aggregateStream(ds: Dataset[FlightRouteRaw]/*, waterMark: String = "2 seconds"*/): DataFrame = {
    ds
      // use an additional watermark column as required for streaming aggregations
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "2 seconds")
      .groupBy(col("timestamp"), col("srcAirport"))
      .count()
  }

  // print from a structured stream
  def printStream(ds: Dataset[Row]): Unit = {
    ds
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }

  // write the stream contents to a csv file
  // def writeRouteStream(outPath: String, checkPointLocation: String, ds: Dataset[Row]) = {
  def writeRouteStream(ds: Dataset[Row], path: String) = {
    ds
      .writeStream
      .format("csv")
      .option("path", path)
      .option("checkpointLocation", "/tmp/checkpoints/")
      .start()
      .awaitTermination()
  }
}
