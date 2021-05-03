package com.schiphol.kiara.assignment

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.schiphol.kiara.assignment.SparkSessionWrapper
import org.apache.spark.sql.catalyst.ScalaReflection
import shared._

// Create a batch Spark job that read in the routes dataset.
// It should create an overview of the top 10 airports used as source airport.
object batch extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    readRoutes()
    .transform(getTop10)
    .write
      .mode("overwrite")
      .option("header", true)
      .csv("./data/out/batch-top10")
  }

  // read data
  def readRoutes(): Dataset[FlightRouteRaw] = {
    import spark.implicits._
    spark.read
      .schema(ScalaReflection.schemaFor[FlightRouteRaw].dataType.asInstanceOf[StructType])
      .option("header", "false")
      .csv("./data/raw")
      .as[FlightRouteRaw]
  }

  // top 10 airports used as source airport
  def getTop10(ds: Dataset[FlightRouteRaw]): DataFrame = {
    ds
      .groupBy(col("srcAirport"))
      .count()
      .sort(col("count").desc)
      .limit(10)
  }

}
