package com.schiphol.kiara.assignment

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import com.schiphol.kiara.assignment.SparkSessionWrapper
import shared._

// Create a batch Spark job that read in the routes dataset.
// It should create an overview of the top 10 airports used as source airport.
object batch extends SparkSessionWrapper {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val ds = readRoutes()
        // clean the route data to take care of nulls and type casts
        .transform(cleanRoutes)
    val top10 = getTop10(ds)
    // Write the output to a filesystem.
    top10.write.mode("overwrite").option("header",true).csv("./data/out/batch-top10")
  }

  // read data
  def readRoutes(): Dataset[FlightRouteRaw] = spark.read
      .option("header", "false")
      .csv("./data/raw/routes.dat")
      // .schema(rawSchema)
      .toDF(cols: _*)
      .as[FlightRouteRaw]

  // clean the route data to take care of nulls and type casts
  def cleanRoutes(df1: Dataset[FlightRouteRaw]): Dataset[FlightRoute] = {
    // The special value \N is used for "NULL" to indicate that no value is available
    val df2 = cols.foldLeft(df1.toDF())((df: Dataset[Row], column: String) => df.withColumn(column, when(col(column) === "\\N",lit(null)).otherwise(col(column))))
    // cast numeric columns to int
    val numericCols = Seq("airlineId", "srcAirportId", "destAirportId", "stops")
    val df3 = numericCols.foldLeft(df2)((df: DataFrame, column: String) => df.withColumn(column,col(column).cast(IntegerType)))
    df3.as[FlightRoute]
  }

  // top 10 airports used as source airport
  // I presently lack sufficient domain knowledge on whether
  // codeshare 'Y' flight routes constitute duplicates in our list.
  // as such I will presently omit a potential filter on this column!
  def getTop10(ds: Dataset[FlightRoute]): DataFrame = {
    ds
      .groupBy(col("srcAirport"))
      .count()
      .sort(col("count").desc)
      .limit(10)
  }

}
