package com.vladkrava.vehicle.auction.stream.processor.model

import com.vladkrava.vehicle.auction.stream.processor.model.BidMapper.{traderIdRowName, traderIdTuple}
import org.apache.spark.sql.Row

/**
 * Auction `RankedCategoryTrader` case class
 *
 */
case class RankedCategoryTrader(traderId: String, category: String, categoryValue: Long, traderRank: Int)

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
