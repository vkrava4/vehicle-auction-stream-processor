package com.vladkrava.vehicle.auction.stream.processor

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Properties

trait SparkApplication {

  val appMasterProperty = "SPARK_MASTER"
  val appMasterDefault = "local[*]"

  def getOrCreateSparkSession(appName: String): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)

    val spark = SparkSession.builder()
      .appName(appName)
      .master(getSparkMaster)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def stopSession(session: SparkSession): Unit = {
    session.stop()
  }

  def getSparkMaster: String = {
    envOrElseConfig(appMasterProperty, appMasterDefault)
  }

  def envOrElseConfig(envVariableName: String, default: String): String = {
    Properties.envOrElse(
      envVariableName.toUpperCase.replaceAll("""\.""", "_"),
      default
    )
  }

  def streamVehicles(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "vehicle.auction")
      .option("startingOffsets", "latest")
      .load()
  }
}
