package com.schiphol.kiara.assignment

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
      .getOrCreate()
  }

}
