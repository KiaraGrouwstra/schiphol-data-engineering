package com.schiphol.kiara.assignment

import com.schiphol.kiara.assignment.batch._
import com.schiphol.kiara.assignment.shared._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File
import scala.reflect.io.Directory

// Use Spark structured streaming to change your job into a streaming job,
// and use the dataset file as a source.
object streaming extends SparkSessionWrapper {
  import spark.implicits._

  val checkPointDirectory = "data/checkpoint"

  def main(args: Array[String]): Unit = {
    // delete output directory if exists
    val outPath = "./data/out/stream"
    new Directory(new File(outPath)).deleteRecursively()

    // remove the checkpoint directory or else it will continue where it left off...
    // TODO: remove it for going into production
    new Directory(new File(checkPointDirectory)).deleteRecursively()

    val ds = readRoutesStream()
      .transform(cleanRoutes)

    println("Is this actually a stream?: " + ds.isStreaming)

    val query = aggregateStream(ds)

    val fileWriter = writeStream(outPath)(query)
    // sorting needs complete mode, which we can use for testing but not to write to csv
    fileWriter.awaitTermination(120000)
  }

  // read stream
  def readRoutesStream(): Dataset[FlightRouteRaw] = spark
    .readStream
    .option("header", "false")
    .schema(rawSchema)
    .option("maxFilesPerTrigger", 1) // Treat a sequence of files as a stream by picking one file at a time
    .option("rowsPerSecond", 100) // use few rows per micro batch as we do not have much data
    .csv("data/raw")
    .as[FlightRouteRaw]

  // aggregate a stream of flight routes to tally source airports used.
  // to keep this operation compatible with writing to disk,
  // this does not yet handle getting the top 10,
  // which is only compatible with operations supporting complete output mode.
  def aggregateStream(ds: Dataset[FlightRoute]): Dataset[Row] = {
    ds
      .toDF()
      // use an additional watermark column as required for streaming aggregations
      .withColumn("timestamp", (current_timestamp().cast(IntegerType) + round(rand() * 60, 0).cast(IntegerType)).cast(TimestampType))
      // our watermark should not matter much as our data is available upfront,
      // but let's say we'll ditch items if they come in a few seconds late
      .withWatermark("timestamp", "10 seconds")
      .groupBy(col("timestamp"), col("srcAirport"))
      .count()
  }

  // write the stream contents to a csv file
  def writeStream[T](outPath: String)(ds: Dataset[T]) = {
    ds
      .writeStream
      .outputMode("append")
      .option("header", true)
      .format("csv")
      .option("path", outPath)
      .option("checkpointLocation", checkPointDirectory)
      .start()
  }

}
