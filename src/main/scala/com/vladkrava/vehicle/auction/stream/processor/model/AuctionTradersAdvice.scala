package com.vladkrava.vehicle.auction.stream.processor.model

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * Auction `AuctionTradersAdvice` case class
 *
 */
case class AuctionTradersAdvice(vehicleId: String, category: String, traders: List[String])

/**
 * Vehicle Mapper object responsible for providing mapping and validation logic from
 * `Dataset` `Row`s to Vehicle case classes
 *
 */
object VehicleMapper {

  private val messageValueColumn = "value"
  private val messageAlias = "message"

  private val vehicleIdTuple: (String, String) = ("vehicle_id", "vehicleId")
  private val categoryTuple: (String, String) = ("category", "category")


  /**
   * Provides `Vehicle` schema with reduces number of fields
   *
   * @return `StructType` `Vehicle` schema
   *
   */
  def schema(): StructType = {
    new StructType()
      .add(vehicleIdRowName(), StringType, nullable = false)
      .add(categoryRowName(), StringType, nullable = false)
  }

  /**
   * Maps `Vehicle` to `AuctionTradersAdvice` case class with processed and broadcasted `RankedCategoryTrader`s
   *
   * @param row                   row of output from `Dataframe`
   * @param rankedCategoryTraders processed and broadcasted array of `RankedCategoryTrader` using which an advice will be performed
   * @param maxRank               represents a maximum number of `RankedCategoryTrader`s which are going to be populated
   *                              to `AuctionTradersAdvice` based on category match
   * @return mapped `AuctionTradersAdvice`s
   *
   */
  def auctionTradersAdvice(row: Row, rankedCategoryTraders: Broadcast[Array[RankedCategoryTrader]], maxRank: Int): AuctionTradersAdvice = {
    val vehicleId = row.getAs[String](vehicleIdRowName())
    val category = row.getAs[String](categoryRowName())

    AuctionTradersAdvice(
      vehicleId,
      category,
      rankedCategoryTraders.value.toList.filter(_.category == category).map(_.traderId).take(maxRank))
  }

  /**
   * Provides a column name which contains a message payload
   *
   */
  def messageValueColumnName(): String = {
    messageValueColumn
  }

  /**
   * Provides a temporary column name (alias) for message payload
   *
   */
  def messageAliasName(): String = {
    messageAlias
  }

  /**
   * Provides a column name for `vehicle_id` message field
   *
   */
  def vehicleIdMessageName(): String = {
    messageAliasName() + "." + vehicleIdTuple._1
  }

  /**
   * Provides a column name for `category` message field
   *
   */
  def categoryMessageName(): String = {
    messageAliasName() + "." + categoryTuple._1
  }

  private def vehicleIdRowName(): String = {
    vehicleIdTuple._1
  }

  private def categoryRowName(): String = {
    categoryTuple._1
  }
}
