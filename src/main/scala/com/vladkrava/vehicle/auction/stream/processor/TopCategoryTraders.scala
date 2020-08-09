package com.vladkrava.vehicle.auction.stream.processor

import com.vladkrava.vehicle.auction.stream.processor.model.BidMapper.{bids, tradersBids}
import com.vladkrava.vehicle.auction.stream.processor.model.TraderMapper.traderCategories
import com.vladkrava.vehicle.auction.stream.processor.model._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
   * Resulted `DataFrame` will be cached i.e persisted with default storage level - `MEMORY_AND_DISK`
   *
   * @param spark        active `SparkSession` - entry point to programming Spark with the Dataset and DataFrame API
   * @param tradersBatch a location of traders batch file
   * @param bidsBatch    a location of bids batch file
   *
   */
  def reprocessTopCategoryTraders(spark: SparkSession, tradersBatch: String, bidsBatch: String): DataFrame = {
    log.info("Reprocessing TopCategoryTraders into a cache")

    import spark.implicits._

    val distinctBids = processDistinctBids(spark, bidsBatch)

    //    TODO: cleanup

    val topCategoryTradersCache = spark.read.json(tradersBatch)
      .flatMap(traderCategories)
      .join(distinctBids, "traderId")
      .groupBy("traderId", "category").agg(org.apache.spark.sql.functions.sum("bidValue"))
      .sort($"sum(bidValue)".desc_nulls_last)
      .cache()

    log.info("TopCategoryTraders reprocessed. Awaiting for next evaluation")

    //    TODO: Optimize resulting dataset and check the results

    topCategoryTradersCache
  }

  def processDistinctBids(spark: SparkSession, bidsBatch: String): Dataset[TraderIdValuePair] = {
    import spark.implicits._

    spark.read.json(bidsBatch)
      .flatMap(bids)
      .distinct()
      .map(tradersBids)
  }
}
