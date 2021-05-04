package com.schiphol.kiara.assignment

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import com.schiphol.kiara.assignment.SparkSessionWrapper

object shared extends SparkSessionWrapper {

  import spark.implicits._

  // columns as described in https://openflights.org/data.html
  case class FlightRoute(
    airline: String, // 2-letter (IATA) or 3-letter (ICAO) code of the airline.
    airlineId: Option[Int], // Unique OpenFlights identifier for airline.
    srcAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the source airport.
    srcAirportId: Option[Int], // Unique OpenFlights identifier for source airport.
    destAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the destination airport.
    destAirportId: Option[Int], // Unique OpenFlights identifier for destination airport.
    codeshare: Option[String], // "Y" if this flight is a codeshare (that is, not operated by Airline, but another carrier), empty otherwise.
    stops: Int, // Number of stops on this flight ("0" for direct)
    equipment: Option[String], // 3-letter codes for plane type(s) generally used on this flight, separated by spaces
  )

  val routeSchema = ScalaReflection.schemaFor[FlightRoute].dataType.asInstanceOf[StructType]

  case class FlightRouteRaw (
    airline: String, // 2-letter (IATA) or 3-letter (ICAO) code of the airline.
    airlineId: Option[String], // Unique OpenFlights identifier for airline.
    srcAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the source airport.
    srcAirportId: Option[String], // Unique OpenFlights identifier for source airport.
    destAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the destination airport.
    destAirportId: Option[String], // Unique OpenFlights identifier for destination airport.
    codeshare: Option[String], // "Y" if this flight is a codeshare (that is, not operated by Airline, but another carrier), empty otherwise.
    stops: String, // Number of stops on this flight ("0" for direct)
    equipment: Option[String], // 3-letter codes for plane type(s) generally used on this flight, separated by spaces
  )

  val rawSchema = ScalaReflection.schemaFor[FlightRouteRaw].dataType.asInstanceOf[StructType]

  val cols = Seq("airline", "airlineId", "srcAirport", "srcAirportId", "destAirport", "destAirportId", "codeshare", "stops", "equipment")

  case class AirportTally(
    srcAirport: String,
    count: Long,
  )

  val tallySchema = ScalaReflection.schemaFor[AirportTally].dataType.asInstanceOf[StructType]

}
