// package com.schiphol.kiara.assignment

// import org.apache.spark.sql._
// import org.apache.spark.sql.streaming._
// import org.apache.spark.streaming._
// import org.scalatest.FunSpec
// import com.github.mrpowers.spark.fast.tests.DataFrameComparer
// import org.apache.spark.sql.types._
// import com.schiphol.kiara.assignment.batch._
// import com.schiphol.kiara.assignment.streaming._
// import com.schiphol.kiara.assignment.window._

// class WindowSpec
//     extends FunSpec
//     with SparkSessionTestWrapper
//     with DataFrameComparer {

//   import spark.implicits._

//   describe(".window") {

//     it("gets the top 10 airports used as source airport over a pre-defined window") {
//       import spark.implicits._

//       // val top10Schema = StructType( Seq(
//       //     StructField("srcAirport", StringType, false),
//       //     StructField("count", LongType, false),
//       // ))

//       // val expectedDF = spark.read
//       //     .schema(top10Schema)
//       //     .option("header", "true")
//       //     .csv("./data/test/window-top10.csv")

//       val reducedStream =
//             readRoutesStream()
//             .transform(cleanRoutes)
//             .as[FlightRoute]
//             .transform(getTop10Window)

//       reducedStream
//           .writeStream
//           .format("memory")
//           .queryName("WindowSpec")
//           .outputMode("complete")
//           .start()
//           .processAllAvailable()

//       val actualDF = spark
//           .sql("select * from WindowSpec")

//       writeRouteStream("./data/test/window-top10.csv")(actualDF)
//       // assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)

//     }

//   }

// }
