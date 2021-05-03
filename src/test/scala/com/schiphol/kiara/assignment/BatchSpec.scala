package com.schiphol.kiara.assignment

import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import batch._
import shared._

class BatchSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  describe(".batch") {

    it("gets the top 10 airports used as source airport") {
      val actualDF = readRoutes().transform(getTop10)
      val schema = actualDF.schema
      val expectedDF = spark.read
          .schema(schema)
          .option("header", "true")
          .csv("./data/test/batch-top10.csv")
      assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)
    }

  }

}
