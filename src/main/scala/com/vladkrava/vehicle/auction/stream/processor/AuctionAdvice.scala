package com.vladkrava.vehicle.auction.stream.processor

import com.vladkrava.vehicle.auction.SparkApplication
import com.vladkrava.vehicle.auction.model.VehicleMapper
import com.vladkrava.vehicle.auction.model.VehicleMapper._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

/**
 * `AuctionAdvice` stream application
 *
 */
object AuctionAdvice extends SparkApplication {

  /**
   * Main execution function of `AuctionAdvice` stream application which incoming stream of vehicle auctions and
   * forwards recommended ranked traders taken from Hive table  for each vehicle to `TradersAdvice` topic
   *
   */
  def main(args: Array[String]): Unit = {
    init()

    val spark = getOrCreateSparkSession(AuctionAdvice.getClass.getName)
    if (spark.sql("SELECT category, category_top_traders, processed_timestamp FROM AUCTION.ranked_traders").count() < 1) {
      System.exit(-1)
    }

    processVehicleStream(spark)
      .start()
      .awaitTermination()
  }

  private def processVehicleStream(spark: SparkSession): DataStreamWriter[Row] = {
    streamVehicles(spark).select(col(messageValueColumnName()).cast(StringType))

      //      Parsing Kafka message
      .select(from_json(col(messageValueColumnName()), VehicleMapper.schema()).as(messageAliasName()))
      .select(vehicleIdMessageName(), categoryMessageName())

      //      combining with resulted category_top_traders
      .join(broadcast(spark.sql("SELECT category, category_top_traders FROM AUCTION.ranked_traders where processed_timestamp " +
        "IN (SELECT max(t1.processed_timestamp) from AUCTION.ranked_traders t1)")), "category")
      .select("vehicle_id", "category_top_traders")
      .select(to_json(struct("*")) as messageValueColumnName())

      //      Writing response to TradersAdvice topic
      .writeStream
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .outputMode("append")
      .option("kafka.bootstrap.servers", getKafkaBootstrapServers)
      .option("topic", getTopicTradersAdvice)
      .option("checkpointLocation", getTradersAdviceCheckpointDir)
  }
}
