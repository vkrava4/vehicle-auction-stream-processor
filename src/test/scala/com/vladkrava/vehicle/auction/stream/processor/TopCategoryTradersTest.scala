package com.vladkrava.vehicle.auction.stream.processor

import com.vladkrava.vehicle.auction.stream.processor.TopCategoryTraders.{getOrCreateSparkSession, processDistinctBids, reprocessTopCategoryTraders, stopSession}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class TopCategoryTradersTest extends AnyFunSuite with BeforeAndAfterEach {

  var sparkSession: SparkSession = _

  override def beforeEach(): Unit = {
    sparkSession = getOrCreateSparkSession("TopCategoryTradersTest")
  }

  override def afterEach(): Unit = {
    stopSession(sparkSession)
  }

  test("Should map all valid bids from 'bids.simple.json' to 'TraderIdValuePair' s") {
    //    GIVEN
    val bidsTestBatchPath = getClass.getResource("/bids.simple.json").getPath
    val rawBidsSource = Source.fromFile(bidsTestBatchPath, "UTF-8")

    //    WHEN
    val idValues = processDistinctBids(sparkSession, bidsTestBatchPath).collect()

    //    THEN
    assert(idValues.length == rawBidsSource.getLines().length)

    rawBidsSource.close()
  }

  test("Should resolve duplicates from 'bids.duplicated.json' to 'TraderIdValuePair' s") {
    //    GIVEN
    val bidsTestBatchWithDuplicatesPath = getClass.getResource("/bids.duplicated.json").getPath
    val bidsTestBatchPath = getClass.getResource("/bids.simple.json").getPath

    val rawBidsDuplicatedSource = Source.fromFile(bidsTestBatchWithDuplicatesPath, "UTF-8")
    val rawBidsSource = Source.fromFile(bidsTestBatchPath, "UTF-8")

    //    Makes sure file contains duplicate lines comparing to 'bids.simple.json'
    val deduplicatedFileLength = rawBidsSource.getLines().length
    val duplicatedFileLength = rawBidsDuplicatedSource.getLines().length
    assert(deduplicatedFileLength < duplicatedFileLength,
      "A file 'bids.duplicated.json' does not contain duplicate lines comparing to 'bids.simple.json'")

    //    WHEN
    val idValues = processDistinctBids(sparkSession, bidsTestBatchWithDuplicatesPath).collect()

    //    THEN
    assert(idValues.length == deduplicatedFileLength)

    rawBidsDuplicatedSource.close()
    rawBidsSource.close()
  }

  test("Should not map broken or null value records from 'bids.broken.json' to 'TraderIdValuePair' s") {
    //    GIVEN
    val bidsTestBrokenBatchPath = getClass.getResource("/bids.broken.json").getPath
    val rawBidsBrokenSource = Source.fromFile(bidsTestBrokenBatchPath, "UTF-8")

    //    WHEN
    val idValues = processDistinctBids(sparkSession, bidsTestBrokenBatchPath).collect()

    //    THEN: idValues should be 0 as 'bids.broken.json' doesnt contain any valid record
    assert(idValues.length == 0, "TraderIdValuePair should be equal to 0 as as 'bids.broken.json' doesnt contain any valid record")

    rawBidsBrokenSource.close()
  }

  test("Should (re) process TopCategoryTraders per category based on 'bids.simple.json' and 'traders.simple.json' batch data") {
    //    GIVEN
    val bidsTestBatchPath = getClass.getResource("/bids.simple.json").getPath
    val tradersTestBatchPath = getClass.getResource("/traders.simple.json").getPath

    //    Expected result: ordered by total bids grouped by vehicle category trader id's
    val expectedResult: Seq[(String, String, Int)] = Seq(
      ("162d0a2b-7c24-4c71-8f20-2dd69bf6cfa7", "Compact", 2),
      ("0e8b3c0f-62e4-4fcb-8729-8c0362d6345c", "Station", 1),
      ("0e8b3c0f-62e4-4fcb-8729-8c0362d6345c", "Coupe", 1)
    )

    //    WHEN
    val actualCategoryTraders = reprocessTopCategoryTraders(sparkSession, tradersTestBatchPath, bidsTestBatchPath).collect()

    //    THEN: expected and actual data match
    assert(actualCategoryTraders.length == expectedResult.length)
    for (actualRow <- actualCategoryTraders) {
      assert(expectedResult.contains(actualRow(0), actualRow(1), actualRow(2)))
    }
  }
}
