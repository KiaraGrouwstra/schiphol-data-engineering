package com.schiphol.kiara.assignment

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local[2]")
      .appName("spark session")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

}
