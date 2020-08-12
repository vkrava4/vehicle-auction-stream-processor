package com.vladkrava.vehicle.auction.stream.processor

import com.vladkrava.vehicle.auction.stream.processor.model.BidMapper.{bids, tradersBidValues}
import com.vladkrava.vehicle.auction.stream.processor.model.RankedCategoryTraderMapper.rankedCategoryTrader
import com.vladkrava.vehicle.auction.stream.processor.model.TraderMapper._
import com.vladkrava.vehicle.auction.stream.processor.model.VehicleMapper._
import com.vladkrava.vehicle.auction.stream.processor.model._
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TopCategoryTraders extends SparkApplication {

  private val log: Logger = Logger.getLogger(TopCategoryTraders.getClass.toString)

  var topCategoryTradersBroadcast: Broadcast[Array[RankedCategoryTrader]] = _

  def main(args: Array[String]): Unit = {
    val spark = getOrCreateSparkSession(TopCategoryTraders.getClass.getName)

    topCategoryTradersBroadcast = spark.sparkContext.broadcast(reprocessTopCategoryTraders(spark, tradersBatch, bidsBatch, 20).collect())

    processVehicleStream(spark)
      .start()
      .awaitTermination()
  }

  def processVehicleStream(spark: SparkSession): DataStreamWriter[Row] = {
    import spark.implicits._

    streamVehicles(spark).select(col(messageValueColumnName()).cast(StringType))
      //      Parsing Kafka message
      .select(from_json(col(messageValueColumnName()), VehicleMapper.schema()).as(messageAliasName()))
      .select(vehicleIdMessageName(), categoryMessageName())
      //      Mapping to the processed entity
      .map(r => auctionTradersAdvice(r, topCategoryTradersBroadcast, 20))
      .select(to_json(struct("*")) as "value")
      //      Writing response to another topic
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "vehicle.auction.traders.advice")
      .option("checkpointLocation", "/tmp/vehicle/auction/traders/advice")

  }

  /**
   * Re-processes or makes initial processing in a batch of top historical traders by category
   * based on `processDistinctBids` results.
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

    val topCategoryTradersResult = bidsFact
      .join(broadcast(tradersDimension), traderIdColumnName())
      .groupBy(traderIdColumnName(), categoryColumnName()).agg(org.apache.spark.sql.functions.sum(bidValueColumnName()).as(categoryValueColumnName()))
      .withColumn(traderRankColumnName(), rank().over(Window.partitionBy(categoryColumnName()).orderBy(categoryValueColumn().desc_nulls_last)))
      .map(rankedCategoryTrader)
      .filter(_.traderRank <= maxTraderRank)

    log.info("TopCategoryTraders reprocessed. Awaiting for next evaluation")

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

  def prepareDistinctTraders(spark: SparkSession, tradersBatch: String): Dataset[Trader] = {
    import spark.implicits._

    spark.read.schema(TraderMapper.schema())
      .json(tradersBatch)
      .flatMap(traderCategories)
      .distinct()
  }
}
