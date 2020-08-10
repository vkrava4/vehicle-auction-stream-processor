package com.vladkrava.vehicle.auction.stream.processor

import com.vladkrava.vehicle.auction.stream.processor.model.BidMapper.{bids, tradersBidValues}
import com.vladkrava.vehicle.auction.stream.processor.model.RankedCategoryTraderMapper.rankedCategoryTrader
import com.vladkrava.vehicle.auction.stream.processor.model.TraderMapper._
import com.vladkrava.vehicle.auction.stream.processor.model._
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.{Dataset, SparkSession}

object TopCategoryTraders extends SparkApplication {

  val log: Logger = Logger.getLogger(TopCategoryTraders.getClass.toString)

  def main(args: Array[String]): Unit = {
    val spark = getOrCreateSparkSession(TopCategoryTraders.getClass.getName)


    stopSession(spark)
  }

  /**
   * Re-processes or makes initial processing in a batch of top historical traders by category
   * based on `processDistinctBids` results.
   *
   * Resulted `Dataset` will be cached i.e persisted with default storage level - `MEMORY_AND_DISK`
   *
   * @param spark         active `SparkSession` - entry point to programming Spark with the Dataset and DataFrame API
   * @param tradersBatch  a location of traders batch file
   * @param bidsBatch     a location of bids batch file
   * @param maxTraderRank defines a maximum rank from of top traders which will be included in the output `Dataset`
   */
  def reprocessTopCategoryTraders(spark: SparkSession, tradersBatch: String, bidsBatch: String, maxTraderRank: Int): Dataset[RankedCategoryTrader] = {
    log.info("Reprocessing TopCategoryTraders into a cache")

    import spark.implicits._

    val bidsFact = prepareDistinctBids(spark, bidsBatch)
    val tradersDimension = prepareDistinctTraders(spark, tradersBatch)

    //    TODO: broadcast tradersDimension
    //    TODO: experiment with  bidsFact repartition by traderId before joining
    //    TODO: coalesce before caching

    val topCategoryTradersCache = bidsFact
      .join(tradersDimension, traderIdColumnName())
      .groupBy(traderIdColumnName(), categoryColumnName()).agg(org.apache.spark.sql.functions.sum(bidValueColumnName()).as(categoryValueColumnName()))
      .withColumn(traderRankColumnName(), rank().over(Window.partitionBy(categoryColumnName()).orderBy(categoryValueColumn().desc_nulls_last)))
      .map(rankedCategoryTrader)
      .filter(_.traderRank <= maxTraderRank)
      .cache()


    log.info("TopCategoryTraders reprocessed. Awaiting for next evaluation")

    topCategoryTradersCache
  }

  def prepareDistinctBids(spark: SparkSession, bidsBatch: String): Dataset[TraderBidValues] = {
    import spark.implicits._

    spark.read.schema(BidMapper.schema())
      .json(bidsBatch)
      .flatMap(bids)
      .distinct()
      .map(tradersBidValues)
  }

  def prepareDistinctTraders(spark: SparkSession, tradersBatch: String): Dataset[Trader] = {
    import spark.implicits._

    spark.read.schema(TraderMapper.schema())
      .json(tradersBatch)
      .flatMap(traderCategories)
      .distinct()
  }
}
