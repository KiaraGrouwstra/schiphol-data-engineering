package com.schiphol.kiara.assignment

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.schiphol.kiara.assignment.SparkSessionWrapper

// Create a batch Spark job that read in the routes dataset.
// It should create an overview of the top 10 airports used as source airport.
object batch extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df = readRoutes()
    // write to folder to use later as stream
    df.write.mode("overwrite").option("header",false).csv("./data/out/routes")
    // clean the route data to take care of nulls and type casts
    val ds = cleanRoutes(df).as[FlightRoute]
    val top10 = getTop10(ds)
    // Write the output to a filesystem.
    top10.write.mode("overwrite").option("header",true).csv("./data/out/batch-top10")
  }

  // columns as described in https://openflights.org/data.html
  case class FlightRoute(
    airline: String, // 2-letter (IATA) or 3-letter (ICAO) code of the airline.
    airlineId: Option[Int], // Unique OpenFlights identifier for airline.
    srcAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the source airport.
    srcAirportId: Option[Int], // Unique OpenFlights identifier for source airport.
    destAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the destination airport.
    destAirportId: Option[Int], // Unique OpenFlights identifier for destination airport.
    codeshare: Option[String], // "Y" if this flight is a codeshare (that is, not operated by Airline, but another carrier), empty otherwise.
    stops: Int, // Number of stops on this flight ("0" for direct)
    equipment: Option[String], // 3-letter codes for plane type(s) generally used on this flight, separated by spaces
  )
  val cols = Seq("airline", "airlineId", "srcAirport", "srcAirportId", "destAirport", "destAirportId", "codeshare", "stops", "equipment")

  // read data
  def readRoutes(): DataFrame = spark.read
      .option("header", "false")
      .csv("./data/raw/routes.dat")
      .toDF(cols: _*)

  // clean the route data to take care of nulls and type casts
  def cleanRoutes(df1: DataFrame): DataFrame = {
    // The special value \N is used for "NULL" to indicate that no value is available
    val df2 = cols.foldLeft(df1)((df: DataFrame, column: String) => df.withColumn(column, when(col(column) === "\\N",lit(null)).otherwise(col(column))))
    // cast numeric columns to int
    val numericCols = Seq("airlineId", "srcAirportId", "destAirportId", "stops")
    val df3 = numericCols.foldLeft(df2)((df: DataFrame, column: String) => df.withColumn(column,col(column).cast(IntegerType)))
    df3
  }

  // top 10 airports used as source airport
  def getTop10(ds: Dataset[FlightRoute]): DataFrame = {
    ds
      .groupBy(col("srcAirport"))
      .count()
      .sort(col("count").desc)
      .limit(10)
  }

}
