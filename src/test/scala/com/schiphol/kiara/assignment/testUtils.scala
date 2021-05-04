package com.schiphol.kiara.assignment

import org.apache.spark.sql._

object testUtils extends SparkSessionTestWrapper {

  def awaitQuery[T](ds: Dataset[T], queryName: String = "myQuery") = {
    ds
        .writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode("complete")
        .start()
        .processAllAvailable()
    spark.sql("select * from " + queryName)
  }

}
