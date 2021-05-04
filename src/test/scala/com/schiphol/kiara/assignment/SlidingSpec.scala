package com.schiphol.kiara.assignment

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.streaming._
import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import batch._
import streaming._
import sliding._
import shared._
import testUtils._

class SlidingSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

  describe(".sliding") {

    it("gets the top 10 airports used as source airport over a pre-defined window") {
      import spark.implicits._

      // val top10Schema = StructType( Seq(
      //     StructField("srcAirport", StringType, false),
      //     StructField("count", LongType, false),
      // ))

      val expectedDF = spark.read
          .schema(tallySchema)
          .option("header", true)
          // .csv("./data/test/top.csv")
          // .csv("./data/test/window-top10.csv")
          .csv("./data/test/batch-top10.csv")

      // val reducedStream =
      //     readRoutesStream()
      //     .transform(cleanRoutes)
      //     .as[FlightRoute]
      //     .transform(getTop10Window)

      val ds = readRoutesStream()
          .transform(cleanRoutes)
      val query = aggregateWindow(ds)

      // reducedStream
      //     .writeStream
      //     .format("memory")
      //     .queryName("WindowSpec")
      //     .outputMode("complete")
      //     .start()
      //     .processAllAvailable()

      // val actualDF = spark
      //     .sql("select * from WindowSpec")
      val actualDF = awaitQuery(query, "sliding")//.select("srcAirport", "count").sort(col("count").desc).limit(10)

      // writeStream("./data/test/window-top10.csv")(actualDF)
      // actualDF.sort(col("count").desc).coalesce(1).write.mode("overwrite").option("header", true).csv("./data/test/window-top10.csv")
      assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true, orderedComparison = false)

    }

  }

}
