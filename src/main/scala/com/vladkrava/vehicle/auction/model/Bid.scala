package com.vladkrava.vehicle.auction.model

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * Auction bid case class
 *
 */
case class Bid(timestamp: Long, traderId: String, vehicleId: String, value: Int)

/**
 * Bid Mapper object responsible for providing mapping and validation logic from
 * `Dataset` `Row`s to Bid case classes
 *
 */
object BidMapper {

  private val timestampTuple: (String, String) = ("bidding_timestamp", "timestamp")
  private val traderIdTuple: (String, String) = ("trader_id", "traderId")
  private val vehicleIdTuple: (String, String) = ("vehicle_id", "vehicleId")

  private val defaultBidValue = 1

  private val timestampLength = 10
  private val uuidLength = 36

  /**
   * Provides logic for flat-mapping a `Dataset` `Row` to Optioned `Bid` case class
   *
   * @param row one row of output from a relational operator
   * @return `Some[Bid]` if row is eligible to be mapper, `None` otherwise
   *
   */
  def bids(row: Row): Option[Bid] = {
    val timestamp = row.getAs[String](timestampRowName())
    val traderId = row.getAs[String](traderIdRowName())
    val vehicleId = row.getAs[String](vehicleIdRowName())

    if (isValid(timestamp, traderId, vehicleId)) {
      Some(Bid(timestamp.toLong, traderId, vehicleId, defaultBidValue))
    } else {
      None
    }
  }

  /**
   * Provides logic for re-mapping a `Bid` to `TraderIdValuePair` case class
   *
   * @note used to remove timestamp and vehicleId columns
   * @param bid an auction bid case class
   * @return `TraderIdValuePair` simplified representation of a `Bid`
   *
   */
  def tradersBidValues(bid: Bid): TraderBidValues = {
    TraderBidValues(bid.traderId, bid.value)
  }

  /**
   * Provides `Bid` schema
   *
   * @return `StructType` `Bid` schema
   *
   */
  def schema(): StructType = {
    new StructType()
      .add(timestampRowName(), StringType, nullable = false)
      .add(traderIdRowName(), StringType, nullable = false)
      .add(vehicleIdRowName(), StringType, nullable = false)
  }

  /**
   * Validates parsed `Bid` `Dataset` fields
   *
   * @param timestamp `Bid` field
   * @param traderId  `Bid` field
   * @param vehicleId `Bid` field
   * @return `true` if parsed fields are valid, `false` otherwise
   *
   */
  private def isValid(timestamp: String, traderId: String, vehicleId: String): Boolean = {
    timestamp != null && timestamp.length == timestampLength &&
      traderId != null && traderId.length == uuidLength &&
      vehicleId != null && vehicleId.length == uuidLength
  }

  private def timestampRowName(): String = {
    timestampTuple._1
  }

  private def traderIdRowName(): String = {
    traderIdTuple._1
  }

  private def vehicleIdRowName(): String = {
    vehicleIdTuple._1
  }
}
