package com.vladkrava.vehicle.auction.model

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{ColumnName, Row}

import scala.collection.mutable

/**
 * Auction `Trader` case class
 *
 */
case class Trader(traderId: String, category: String)

/**
 * `TraderId` case class, contains only traderIds
 *
 */
case class TraderId(traderId: String)


/**
 * `TraderValues` case class, represents a historical bid value of each trader or a value for each `Bid`
 *
 */
case class TraderBidValues(traderId: String, bidValue: Int)

/**
 * Auction `RankedCategoryTrader` case class
 *
 */
case class RankedCategoryTrader(traderId: String, category: String, categoryValue: Long, traderRank: Int)


/**
 * Trader Mapper object responsible for providing mapping and validation logic from
 * `Dataset` `Row`s to Trader case classes
 *
 */
object TraderMapper {

  private val traderIdTuple: (String, String) = ("trader_id", "traderId")
  private val categoryTuple: (String, String) = ("specialised_categories", "category")
  private val bidValueTuple: (String, String) = ("", "bidValue")
  private val categoryValueTuple: (String, String) = ("", "categoryValue")
  private val traderRankTuple: (String, String) = ("", "traderRank")

  private val uuidLength = 36
  private val categoriesDelimiter = '/'

  /**
   * Provides logic for flat-mapping a `Dataset` `Row` to List of `Trader`s where traderId has one-to-many
   * relationship with its categories
   *
   * @param row one row of output from a relational operator
   * @return `List[Trader]`s
   *
   */
  def traderCategories(row: Row): List[Trader] = {
    val tradersPerCategory: mutable.MutableList[Trader] = mutable.MutableList()

    val traderId = row.getAs[String](traderIdRowName())
    val rawCategories = row.getAs[String](categoryRowName())

    if (isValid(traderId, rawCategories)) {
      for (category: String <- rawCategories.split(categoriesDelimiter).toSet) {
        tradersPerCategory += Trader(traderId, category)
      }
    }

    tradersPerCategory.toList
  }

  /**
   * Provides `Trader` schema
   *
   * @return `StructType` `Trader` schema
   *
   */
  def schema(): StructType = {
    new StructType()
      .add(categoryRowName(), StringType, nullable = false)
      .add(traderIdRowName(), StringType, nullable = false)
  }

  /**
   * Validates parsed `Trader` `Dataset` fields
   *
   * @param traderId      `Trader` field
   * @param rawCategories `Trader` field
   * @return `true` if parsed fields are valid, `false` otherwise
   *
   */
  private def isValid(traderId: String, rawCategories: String): Boolean = {
    traderId != null && traderId.length == uuidLength &&
      rawCategories != null && rawCategories.length > 0
  }

  /**
   * Provides logic for re-mapping a `Trader` to `TraderId` case class
   *
   * @note used to remove category columns
   * @param trader an auction `Trader` case class
   * @return `TraderId` simplified representation of a `Trader`
   *
   */
  def traderIds(trader: Trader): TraderId = {
    TraderId(trader.traderId)
  }

  /**
   * Provides Trader Id Dataset `Column`
   *
   * @return Trader Id Dataset `Column`
   */
  def traderIdColumn(): ColumnName = {
    new ColumnName(traderIdTuple._2)
  }

  /**
   * Provides Trader Id column name of a `Trader` case class in `String` format
   *
   * @return Trader Id column name
   */
  def traderIdColumnName(): String = {
    traderIdTuple._2
  }

  /**
   * Provides Category column name of a `Trader` case class in `String` format
   *
   * @return Category column name
   */
  def categoryColumnName(): String = {
    categoryTuple._2
  }

  /**
   * Provides Bid Value column name of a `Trader` case class in `String` format
   *
   * @return Bid Value column name
   */
  def bidValueColumnName(): String = {
    bidValueTuple._2
  }

  /**
   * Provides Category Value column name of a `Trader` case class in `String` format
   *
   * @return Category Value column name
   */
  def categoryValueColumnName(): String = {
    categoryValueTuple._2
  }

  /**
   * Provides Category Dataset `Column`
   *
   * @return Category Dataset `Column`
   */
  def categoryColumn(): ColumnName = {
    new ColumnName(categoryTuple._2)
  }

  /**
   * Provides Category Value Dataset `Column`
   *
   * @return Category Value Dataset `Column`
   */
  def categoryValueColumn(): ColumnName = {
    new ColumnName(categoryValueTuple._2)
  }

  /**
   * Provides Trader Rank column name in `String` format
   *
   * @return Trader Rank column name
   */
  def traderRankColumnName(): String = {
    traderRankTuple._2
  }

  private def traderIdRowName(): String = {
    traderIdTuple._1
  }

  private def categoryRowName(): String = {
    categoryTuple._1
  }
}

object RankedCategoryTraderMapper {

  private val traderIdTuple: (String, String) = ("traderId", "traderId")
  private val categoryTuple: (String, String) = ("category", "category")
  private val categoryValueTuple: (String, String) = ("categoryValue", "categoryValue")
  private val traderRankTuple: (String, String) = ("traderRank", "traderRank")

  def rankedCategoryTrader(row: Row): RankedCategoryTrader = {
    val traderId = row.getAs[String](traderIdRowName())
    val category = row.getAs[String](categoryRowName())
    val categoryValue = row.getAs[Long](categoryValueRowName())
    val traderRank = row.getAs[Int](traderRankRowName())

    RankedCategoryTrader(traderId, category, categoryValue, traderRank)
  }

  private def traderIdRowName(): String = {
    traderIdTuple._1
  }

  private def categoryRowName(): String = {
    categoryTuple._1
  }

  private def categoryValueRowName(): String = {
    categoryValueTuple._1
  }

  private def traderRankRowName(): String = {
    traderRankTuple._1
  }
}
