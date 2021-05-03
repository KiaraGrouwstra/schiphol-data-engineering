package com.schiphol.kiara.assignment

object shared {

  case class FlightRouteRaw(
    airline: String, // 2-letter (IATA) or 3-letter (ICAO) code of the airline.
    var airlineId: Option[String] = None, // Unique OpenFlights identifier for airline.
    srcAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the source airport.
    var srcAirportId: Option[String] = None, // Unique OpenFlights identifier for source airport.
    destAirport: String, // 3-letter (IATA) or 4-letter (ICAO) code of the destination airport.
    var destAirportId: Option[String] = None, // Unique OpenFlights identifier for destination airport.
    var codeshare: Option[String] = None, // "Y" if this flight is a codeshare (that is, not operated by Airline, but another carrier), empty otherwise.
    stops: Int, // Number of stops on this flight ("0" for direct)
    var equipment: Option[String] = None, // 3-letter codes for plane type(s) generally used on this flight, separated by spaces
  ) {
    if (airlineId.get == "\\N")     airlineId     = Option.empty
    if (srcAirportId.get == "\\N")  srcAirportId  = Option.empty
    if (destAirportId.get == "\\N") destAirportId = Option.empty
    if (codeshare.get == "\\N")     codeshare     = Option.empty
    if (equipment.get == "\\N")     equipment     = Option.empty
  }

}
