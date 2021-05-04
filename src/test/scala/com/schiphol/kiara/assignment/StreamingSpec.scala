package com.schiphol.kiara.assignment

import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.streaming._
import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import batch._
import streaming._
import shared._
import testUtils._

class StreamingSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

  describe(".streaming") {

    it("streams a tally of source airports by number of flights") {
      import spark.implicits._

      val expectedDF = spark.read
          .schema(tallySchema)
          .option("header", "true")
          .csv("./data/test/stream-top.csv")

      val df = readRoutesStream()
        .transform(cleanRoutes)
      val query = aggregateStream(df)

      val actualDF = awaitQuery(query, "streaming")
          // .select("srcAirport", "count")
          .sort(col("count").desc)
          // .limit(10)

      assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true, orderedComparison = false)

    }

  }

}
