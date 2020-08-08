package com.vladkrava.vehicle.auction.stream.processor

object TopCategoryTraders extends SparkApplication {

  def main(args: Array[String]): Unit = {
    val spark = getOrCreateSparkSession(TopCategoryTraders.getClass.getName)
    import spark.implicits._

    val data = Seq("test", "test2")

    val testDs = spark.createDataset(data)
    testDs.take(2).foreach(println)

    stopSession(spark)
  }
}
