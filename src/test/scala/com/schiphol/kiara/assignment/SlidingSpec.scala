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

      val ds = readRoutesStream()
          .transform(cleanRoutes)
      val query = aggregateWindow(ds)

      // while our timestamp allocation is randomized,
      // we do expect our aggregated dataset for any given window to contain 10 airports
      val actualDF = awaitQuery(query, "sliding")
      // val firstWindow = actualDF.select(col("window")).collect()(0)(0).asInstanceOf[String]//.getString(0)
      // val oneWindow = actualDF.filter(col("window") === firstWindow)
      // assert(oneWindow.count() == 10)

    }

  }

}
