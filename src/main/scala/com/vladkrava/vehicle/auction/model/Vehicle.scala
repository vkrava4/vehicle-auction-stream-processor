package com.vladkrava.vehicle.auction.model

import org.apache.spark.sql.types.{StringType, StructType}

/**
 * Vehicle Mapper object responsible for providing mapping and validation logic from
 * `Dataset` `Row`s to Vehicle case classes
 *
 */
object VehicleMapper {

  private val messageValueColumn = "value"
  private val messageAlias = "message"

  private val vehicleIdTuple: (String, String) = ("vehicle_id", "vehicleId")
  private val categoryTuple: (String, String) = ("category", "category")


  /**
   * Provides `Vehicle` schema with reduces number of fields
   *
   * @return `StructType` `Vehicle` schema
   *
   */
  def schema(): StructType = {
    new StructType()
      .add(vehicleIdRowName(), StringType, nullable = false)
      .add(categoryRowName(), StringType, nullable = false)
  }

  /**
   * Provides a column name which contains a message payload
   *
   */
  def messageValueColumnName(): String = {
    messageValueColumn
  }

  /**
   * Provides a temporary column name (alias) for message payload
   *
   */
  def messageAliasName(): String = {
    messageAlias
  }

  /**
   * Provides a column name for `vehicle_id` message field
   *
   */
  def vehicleIdMessageName(): String = {
    messageAliasName() + "." + vehicleIdTuple._1
  }

  /**
   * Provides a column name for `category` message field
   *
   */
  def categoryMessageName(): String = {
    messageAliasName() + "." + categoryTuple._1
  }

  private def vehicleIdRowName(): String = {
    vehicleIdTuple._1
  }

  private def categoryRowName(): String = {
    categoryTuple._1
  }
}
