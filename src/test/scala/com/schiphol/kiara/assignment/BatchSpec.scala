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

  import spark.implicits._

  describe(".batch") {

    it("gets the top 10 airports used as source airport") {
      import spark.implicits._
      val ds = readRoutes()
          .transform(cleanRoutes)
      val actualDF = ds.transform(getTop10)
      val schema = actualDF.schema
      val expectedDF = spark.read
          .schema(schema)
          .option("header", "true")
          .csv("./data/test/batch-top10.csv")
      assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)
    }

  }

}
