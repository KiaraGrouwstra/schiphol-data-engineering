package com.schiphol.kiara.assignment

import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import com.schiphol.kiara.assignment.SparkSessionWrapper
import batch._

// Use Spark structured streaming to change your job into a streaming job, and use the dataset file
// as a source.
object streaming extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    // delete output directory if exists
    val outPath = "./data/out/stream-top10"
    new Directory(new File(outPath)).deleteRecursively()

    val df = readRoutesStream()
      .transform(cleanRoutes)
      .as[FlightRoute]
      .transform(getTop10Stream)
    writeRouteStream(outPath)(df)
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
      .option("header","true")
      .schema(schema)
      .csv("./data/out/batch-routes")

  // top 10 airports used as source airport
  // using an additional watermark column as required for streaming
  def getTop10Stream(ds: Dataset[FlightRoute]): DataFrame = {
    ds
      .withColumn("timestamp", current_timestamp())
      // our watermark should not matter much as our data is available upfront,
      // but let's say we'll ditch items if they come in a minute late
      .withWatermark("timestamp", "1 minutes")
      .groupBy(col("timestamp"), col("srcAirport"))
      .count()
      // sorting needs complete mode, which we can use for testing but not to write to csv
      // .sort(col("count").desc)
      .limit(10)
  }

  // write the stream contents to a csv file
  def writeRouteStream(outPath: String)(ds: Dataset[Row]) = {
    ds
      .writeStream
      .option("header",true)
      .format("csv")
      .option("path", outPath)
      .option("checkpointLocation", "/tmp/checkpoints/")
      .start()
      .awaitTermination()
  }

}
