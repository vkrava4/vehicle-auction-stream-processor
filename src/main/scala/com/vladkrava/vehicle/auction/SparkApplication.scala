package com.vladkrava.vehicle.auction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark Application
 *
 */
trait SparkApplication extends CoreApplication {

  def getOrCreateSparkSession(appName: String): SparkSession = {
    val sparkBuilder = SparkSession.builder().appName(appName)

    if (sparkMaster.isDefined) {

      //      Do not enable Hive for local Spark
      sparkBuilder.master(sparkMaster.get)
      if (!sparkMaster.get.contains("local")) {
        sparkBuilder.enableHiveSupport()
      }
    }

    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)

    spark
  }

  def stopSession(session: SparkSession): Unit = {
    session.stop()
  }

  def streamVehicles(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", getKafkaBootstrapServers)
      .option("subscribe", getTopicVehicleAuction)
      .option("startingOffsets", "latest")
      .load()
  }
}
