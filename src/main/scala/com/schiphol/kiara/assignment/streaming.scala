package com.schiphol.kiara.assignment

import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.catalyst.ScalaReflection
import com.schiphol.kiara.assignment.SparkSessionWrapper
import shared._
import batch._

// Use Spark structured streaming to change your job into a streaming job,
// and use the dataset file as a source.
object streaming extends SparkSessionWrapper {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // delete output directory if exists
    val outPath = "./data/out/stream"
    new Directory(new File(outPath)).deleteRecursively()

    val ds = readRoutesStream()
      .transform(cleanRoutes)
    val query = aggregateStream(ds)
    val fileWriter = writeStream(outPath)(query)
    // sorting needs complete mode, which we can use for testing but not to write to csv
    // print top 10 airports used as source airport
    val printWriter = printStream(query.sort(col("count").desc).limit(10))
    fileWriter.awaitTermination()
    printWriter.awaitTermination()
  }

  // read stream
  def readRoutesStream(): Dataset[FlightRouteRaw] = spark
      .readStream
      .option("header","false")
      .schema(rawSchema)
      .option("rowsPerSecond", 100) // use few rows per micro batch as we do not have much data
      .csv("./data/raw")
      .as[FlightRouteRaw]

  // aggregate a stream of flight routes to tally source airports used.
  // to keep this operation compatible with writing to disk,
  // this does not yet handle getting the top 10,
  // which is only compatible with operations supporting complete output mode.
  def aggregateStream(ds: Dataset[FlightRoute]): Dataset[Row] = {
    ds
      .toDF()
      // use an additional watermark column as required for streaming aggregations
      .withColumn("timestamp", current_timestamp())
      // our watermark should not matter much as our data is available upfront,
      // but let's say we'll ditch items if they come in a few seconds late
      .withWatermark("timestamp", "2 seconds")
      .groupBy(col("timestamp"), col("srcAirport"))
      .count()
  }

  // print from a structured stream
  def printStream[T](ds: Dataset[T]) = {
    ds
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
  }

  // write the stream contents to a csv file
  def writeStream[T](outPath: String)(ds: Dataset[T]) = {
    ds
      .writeStream
      .outputMode("append")
      .option("header", true)
      .format("csv")
      .option("path", outPath)
      .option("checkpointLocation", "/tmp/checkpoints/")
      .start()
  }

}
