package com.schiphol.kiara.assignment

import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import com.schiphol.kiara.assignment.SparkSessionWrapper
import batch._

// Use Spark structured streaming to change your job into a streaming job,
// and use the dataset file as a source.
object streaming extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    // delete output directory if exists
    val outPath = "./data/out/stream-top10"
    new Directory(new File(outPath)).deleteRecursively()

    val df = readRoutesStream()
      .transform(cleanRoutes)
      .as[FlightRoute]
    val query = aggregateStream(df)
    val fileWriter = writeRouteStream(outPath)(query)
    // sorting needs complete mode, which we can use for testing but not to write to csv
    // print top 10 airports used as source airport
    val printWriter = printStream(query.sort(col("count").desc).limit(10))
    fileWriter.awaitTermination()
    printWriter.awaitTermination()
  }

  // schema required to read the raw input data in a streaming context
  val schema = StructType( Seq(
      StructField("airline", StringType, false),
      StructField("airlineId", StringType, true),
      StructField("srcAirport", StringType, false),
      StructField("srcAirportId", StringType, true),
      StructField("destAirport", StringType, false),
      StructField("destAirportId", StringType, true),
      StructField("codeshare", StringType, true),
      StructField("stops", StringType, false),
      StructField("equipment", StringType, true),
  ))

  // read stream
  // assignment: use the dataset file as a source.
  // unfortunately, we cannot read streams from plain CSVs,
  // so instead we use a foldered version from our batch script.
  def readRoutesStream(): DataFrame = spark
      .readStream
      .option("header","false")
      .schema(schema)
      .option("rowsPerSecond", 1) // use 1 row per micro batch because we do not have much data
      .csv("./data/out/batch-routes")

  // aggregate a stream of flight routes to tally source airports used.
  // to keep this operation compatible with writing to disk,
  // this does not yet handle getting the top 10,
  // which is only compatible with operations supporting complete output mode.
  def aggregateStream(ds: Dataset[FlightRoute]): DataFrame = {
    ds
      // use an additional watermark column as required for streaming aggregations
      .withColumn("timestamp", current_timestamp())
      // our watermark should not matter much as our data is available upfront,
      // but let's say we'll ditch items if they come in a few seconds late
      .withWatermark("timestamp", "2 seconds")
      .groupBy(col("timestamp"), col("srcAirport"))
      .count()
  }

  // print from a structured stream
  def printStream(df: DataFrame) = {
    df
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
  }

  // write the stream contents to a csv file
  def writeRouteStream(outPath: String)(ds: Dataset[Row]) = {
    ds
      .writeStream
      .outputMode("append")
      .option("header",true)
      .format("csv")
      .option("path", outPath)
      .option("checkpointLocation", "/tmp/checkpoints/")
      .start()
  }

}
