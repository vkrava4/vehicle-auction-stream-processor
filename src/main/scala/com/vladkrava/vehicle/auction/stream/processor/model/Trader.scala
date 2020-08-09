package com.vladkrava.vehicle.auction.stream.processor.model

import org.apache.spark.sql.Row

import scala.collection.mutable

case class Trader(traderId: String, category: String)
case class TraderId(traderId: String)
case class TraderIdValuePair(traderId: String, bidValue: Int)

object TraderMapper {

  def traderCategories(row: Row): List[Trader] = {
    var tradersPerCategory: mutable.MutableList[Trader] = mutable.MutableList()

    val traderId = row.getAs[String]("trader_id")
    val rawCategories = row.getAs[String]("specialised_categories")

    for (category: String <- rawCategories.split('/').toSet) {
      tradersPerCategory += Trader(traderId, category)
    }

    tradersPerCategory.toList
  }

  def traderIds(trader: Trader): TraderId = {
    TraderId(trader.traderId)
  }
}
