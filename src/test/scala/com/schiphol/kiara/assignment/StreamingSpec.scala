package com.schiphol.kiara.assignment

import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.streaming._
import org.apache.spark.streaming._
import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import com.schiphol.kiara.assignment.batch._
import com.schiphol.kiara.assignment.streaming._

class StreamingSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

  describe(".streaming") {

    it("streams a tally of source airports by number of flights") {
      import spark.implicits._

      val top10Schema = StructType( Seq(
          StructField("srcAirport", StringType, false),
          StructField("count", LongType, false),
      ))

      val expectedDF = spark.read
          .schema(top10Schema)
          .option("header", "true")
          .csv("./data/test/top.csv")

      val df = readRoutesStream()
        .transform(cleanRoutes)
        .as[FlightRoute]
      val query = aggregateStream(df)

      query
          .writeStream
          .format("memory")
          .queryName("WindowSpec")
          .outputMode("complete")
          .start()
          .processAllAvailable()

      val actualDF = spark
          .sql("select srcAirport, count from WindowSpec")

      assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true, orderedComparison = false)

    }

  }

}
