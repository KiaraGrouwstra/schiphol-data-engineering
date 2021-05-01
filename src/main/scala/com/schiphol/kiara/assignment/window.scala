package com.schiphol.kiara.assignment

import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import com.schiphol.kiara.assignment.SparkSessionWrapper
import batch._
import streaming._

// Next change your streaming job so the aggregations are done using sliding windows. Pick any
// window and sliding interval. The end result should be the top 10 airports used as source airport
// within each window. When choosing the window interval, keep the size of the dataset in mind.
object window extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    // delete output directory if exists
    val outPath = "./data/out/window-top10"
    new Directory(new File(outPath)).deleteRecursively()

    val df = readRoutesStream()
      .transform(cleanRoutes)
      .as[FlightRoute]
      .transform(getTop10Window)
    writeRouteStream(outPath)(df)
  }

  // top 10 airports used as source airport
  // the aggregations are done using sliding windows. Pick any window and sliding interval.
  // The end result should be the top 10 airports used as source airport within each window.
  // When choosing the window interval, keep the size of the dataset in mind.
  def getTop10Window(ds: Dataset[FlightRoute]): DataFrame = {
    ds
      // use monotonically increasing IDs such as to have distinct values we could make a window over
      .withColumn("timestamp", monotonicallyIncreasingId)
      .withWatermark("timestamp", "1 minutes")
      // interpreting the ids as a unix timestamp, which is measured in seconds,
      // a window of 1 hour would include 3600 entries, i.e. around 5% of the dataset.
      // as a sliding window we will (arbitrarily) pick half of that,
      // such as to ensure we will not get too much data,
      // as we might using a small sliding window.
      .groupBy(org.apache.spark.sql.functions.window(col("timestamp"), "1 hour", "30 minutes"), col("srcAirport"))
      .count()
      // sorting needs complete mode, which we can use for testing but not to write to csv
      // .sort(col("count").desc)
      .limit(10)
  }

}
