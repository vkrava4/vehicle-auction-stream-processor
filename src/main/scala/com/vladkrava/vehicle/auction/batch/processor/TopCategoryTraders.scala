package com.vladkrava.vehicle.auction.batch.processor

import com.vladkrava.vehicle.auction.SparkApplication
import com.vladkrava.vehicle.auction.model.BidMapper.{bids, tradersBidValues}
import com.vladkrava.vehicle.auction.model.RankedCategoryTraderMapper.rankedCategoryTrader
import com.vladkrava.vehicle.auction.model.TraderMapper._
import com.vladkrava.vehicle.auction.model._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, collect_list, dense_rank, rank, row_number}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * `TopCategoryTraders` bulk job
 *
 */
object TopCategoryTraders extends SparkApplication {

  /**
   * Main execution function of `TopCategoryTraders` bulk job which processes ranked (top) Traders based on provided
   * parameter(s) and loads resulted output into Hive's results table
   *
   * @param args (0) an argument parameter which defines
   *
   */
  def main(args: Array[String]): Unit = {
    init()

    val spark = getOrCreateSparkSession(TopCategoryTraders.getClass.getName)

    val categoryTraders = reprocessTopCategoryTraders(spark, getTradersBatchPath, getBidsBatchPath, args(0).toInt)

    //      Normalising results before storing by category
    categoryTraders.groupBy(categoryColumn().as(categoryColumnName())).agg(collect_list("traderId").cast(StringType).as("category_top_traders"))
      .write
      .format("orc")
      .mode("overwrite")
      .saveAsTable("AUCTION.ranked_traders_tmp")

    spark.sql("INSERT INTO AUCTION.ranked_traders SELECT category, category_top_traders, unix_timestamp() from AUCTION.ranked_traders_tmp")
  }

  /**
   * Re-processes or makes initial processing in a batch of top historical traders by category
   * based on `processDistinctBids` results.
   *
   * @note for ranking a `row_number` over `rank` or `dense_rank` is chosen in order to leave no gaps or collisions in a ranking sequence,
   *       i.e to avoid having the same rank for multiple traders with identical values
   * @note no `coalesce` is done after filtering stage for performance reasons, as resulted `Dataset` will be loaded to
   *       Hive which eliminates a need of this
   * @param spark         active `SparkSession` - entry point to programming Spark with the Dataset and DataFrame API
   * @param tradersBatch  a location of traders batch file
   * @param bidsBatch     a location of bids batch file
   * @param maxTraderRank defines a maximum rank from of top traders which will be included in the output `Dataset`
   */
  def reprocessTopCategoryTraders(spark: SparkSession, tradersBatch: String, bidsBatch: String, maxTraderRank: Int): Dataset[RankedCategoryTrader] = {
    import spark.implicits._

    val bidsFact = prepareDistinctBids(spark, bidsBatch)
    val tradersDimension = prepareDistinctTraders(spark, tradersBatch)

    val topCategoryTradersResult = bidsFact
      .join(broadcast(tradersDimension), traderIdColumnName())
      .groupBy(traderIdColumnName(), categoryColumnName()).agg(org.apache.spark.sql.functions.sum(bidValueColumnName()).as(categoryValueColumnName()))
      .withColumn(traderRankColumnName(), row_number().over(Window.partitionBy(categoryColumnName()).orderBy(categoryValueColumn().desc_nulls_last)))
      .map(rankedCategoryTrader)
      .filter(_.traderRank <= maxTraderRank)

    topCategoryTradersResult
  }

  def prepareDistinctBids(spark: SparkSession, bidsBatch: String): Dataset[TraderBidValues] = {
    import spark.implicits._

    spark.read.schema(BidMapper.schema())
      .json(bidsBatch)
      .flatMap(bids)
      .distinct()
      .map(tradersBidValues)
  }

  /**
   * Mapping, de-normalising and de-duplicating Traders dimension table
   *
   * @note no repartition is done in this step for performance reasons, as resulted `Dataset`
   *       will be broadcasted for fact table
   *
   */
  def prepareDistinctTraders(spark: SparkSession, tradersBatch: String): Dataset[Trader] = {
    import spark.implicits._

    spark.read.schema(TraderMapper.schema())
      .json(tradersBatch)
      .flatMap(traderCategories)
      .distinct()
  }
}
