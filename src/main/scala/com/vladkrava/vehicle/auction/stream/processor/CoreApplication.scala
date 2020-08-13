package com.vladkrava.vehicle.auction.stream.processor

import scala.util.Properties

/**
 * Core Application
 *
 */
trait CoreApplication {

  //  Values
  private var appKafkaBootstrapValue: String = _
  private var appTopicVehicleAuctionValue: String = _
  private var appTopicTradersAdviceValue: String = _
  private var appTopicTradersAdviceCheckpointDirValue: String = _
  private var appBidsBatchFileValue: String = _
  private var appTradersBatchFileValue: String = _

  //  ENV variables
  private val appMasterVarName = "SPARK_MASTER"
  private val appKafkaBootstrapVarName = "KAFKA_BOOTSTRAP_SERVERS"
  private val appTopicVehicleAuctionVarName = "TOPIC_VEHICLE_AUCTION"
  private val appTopicTradersAdviceVarName = "TOPIC_TRADERS_ADVICE"
  private val appTopicTradersAdviceCheckpointDirVarName = "TRADERS_ADVICE_CHECKPOINT_DIR"
  private val appBidsBatchFileVarName = "BIDS_BATCH"
  private val appTradersBatchFileVarName = "TRADERS_BATCH"

  //  Default values
  private val appTopicVehicleAuctionDefault: String = "vehicle.auction"
  private val appTopicTradersAdviceDefault: String = "vehicle.auction.traders.advice"
  private val appTopicTradersAdviceCheckpointDirDefault: String = "/tmp/vehicle_auction_traders_advice"

  /**
   * Initialises Core Application
   *
   */
  def init(): Unit = {
    appKafkaBootstrapValue = envOrNone(appKafkaBootstrapVarName).orNull
    appBidsBatchFileValue = envOrNone(appBidsBatchFileVarName).orNull
    appTradersBatchFileValue = envOrNone(appTradersBatchFileVarName).orNull

    appTopicVehicleAuctionValue = envOrNone(appTopicVehicleAuctionVarName).getOrElse(appTopicVehicleAuctionDefault)
    appTopicTradersAdviceValue = envOrNone(appTopicTradersAdviceVarName).getOrElse(appTopicTradersAdviceDefault)
    appTopicTradersAdviceCheckpointDirValue = envOrNone(appTopicTradersAdviceCheckpointDirVarName).getOrElse(appTopicTradersAdviceCheckpointDirDefault)

    validate()
  }


  /**
   * Provides `BIDS_BATCH` env configuration with bids batch file location
   *
   */
  def getBidsBatchPath: String = {
    appBidsBatchFileValue
  }

  /**
   * Provides `TRADERS_BATCH` env configuration with traders batch file location
   *
   */
  def getTradersBatchPath: String = {
    appTradersBatchFileValue
  }

  /**
   * Provides `KAFKA_BOOTSTRAP_SERVERS` env configuration for `kafka.bootstrap.servers`
   *
   */
  def getKafkaBootstrapServers: String = {
    appKafkaBootstrapValue
  }

  /**
   * Provides `TOPIC_VEHICLE_AUCTION` env configuration vehicle auction topic name
   *
   */
  def getTopicVehicleAuction: String = {
    appTopicVehicleAuctionValue
  }

  /**
   * Provides `TOPIC_TRADERS_ADVICE` env configuration traders advice topic name
   *
   */
  def getTopicTradersAdvice: String = {
    appTopicTradersAdviceValue
  }

  /**
   * Provides `TRADERS_ADVICE_CHECKPOINT_DIR` env configuration for `checkpointLocation`
   *
   */
  def getTradersAdviceCheckpointDir: String = {
    appTopicTradersAdviceCheckpointDirValue
  }

  /**
   * Provides `SPARK_MASTER` env configuration
   *
   */
  def sparkMaster: Option[String] = {
    envOrNone(appMasterVarName)
  }

  private def validate() {
    var valid = true
    val defaultValidationErrorMessage = "has not been configured as ENV variable."

    if (isBlank(appKafkaBootstrapValue)) {
      valid = false
    }

    if (isBlank(appBidsBatchFileValue)) {
      valid = false
    }

    if (isBlank(appTradersBatchFileValue)) {
      valid = false
    }

    if (!valid) {
      System.exit(-1)
    }
  }

  def envOrNone(envVariableName: String): Option[String] = {
    Properties.envOrNone(envVariableName.toUpperCase.replaceAll("""\.""", "_"))
  }

  def isBlank(s: String): Boolean = {
    s == null || s.trim.isEmpty
  }
}
