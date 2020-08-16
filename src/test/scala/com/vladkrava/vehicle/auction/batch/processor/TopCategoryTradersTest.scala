package com.vladkrava.vehicle.auction.batch.processor

import com.vladkrava.vehicle.auction.batch.processor.TopCategoryTraders.{prepareDistinctBids, prepareDistinctTraders, reprocessTopCategoryTraders}
import com.vladkrava.vehicle.auction.model.RankedCategoryTrader
import com.vladkrava.vehicle.auction.stream.processor.AuctionAdvice.{getOrCreateSparkSession, stopSession}
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
    val idValues = prepareDistinctBids(sparkSession, bidsTestBatchPath).collect()

    //    THEN
    assert(idValues.length == rawBidsSource.getLines().length)

    rawBidsSource.close()
  }

  test("Should map all valid traders from 'traders.simple.json' to 'Trader' s") {
    //    GIVEN
    //    expected number of denormalised traders in traders.simple.json
    val expectedDenormalisedTraders = 3
    val tradersTestBatchPath = getClass.getResource("/traders.simple.json").getPath

    //    WHEN
    val traders = prepareDistinctTraders(sparkSession, tradersTestBatchPath).collect()

    //    THEN
    assert(traders.length == expectedDenormalisedTraders)
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
    val idValues = prepareDistinctBids(sparkSession, bidsTestBatchWithDuplicatesPath).collect()

    //    THEN
    assert(idValues.length == deduplicatedFileLength)

    rawBidsDuplicatedSource.close()
    rawBidsSource.close()
  }

  test("Should resolve duplicates from 'traders.duplicated.json' to 'Trader' s") {
    //    GIVEN
    val tradersTestBatchWithDuplicatesPath = getClass.getResource("/traders.duplicated.json").getPath
    val tradersTestBatchWithoutDuplicatesPath = getClass.getResource("/traders.simple.json").getPath

    //    WHEN
    val actualTraders = prepareDistinctTraders(sparkSession, tradersTestBatchWithDuplicatesPath).collect()

    //    THEN
    val deduplicatedReferenceTraders = prepareDistinctTraders(sparkSession, tradersTestBatchWithoutDuplicatesPath).collect()
    assert(actualTraders.length == deduplicatedReferenceTraders.length)
  }

  test("Should not map broken or null value records from 'bids.broken.json' to 'TraderIdValuePair' s while processDistinctBids") {
    //    GIVEN
    val bidsTestBrokenBatchPath = getClass.getResource("/bids.broken.json").getPath

    //    WHEN
    val idValues = prepareDistinctBids(sparkSession, bidsTestBrokenBatchPath).collect()

    //    THEN: idValues should be 0 as 'bids.broken.json' doesnt contain any valid record
    assert(idValues.length == 0, "TraderIdValuePair should be equal to 0 as as 'bids.broken.json' doesnt contain any valid record")
  }

  test("Should not map broken or null value records from 'traders.broken.json' to 'Traders' s while prepareDistinctTraders") {
    //    GIVEN
    val tradersTestBrokenBatchPath = getClass.getResource("/traders.broken.json").getPath

    //    WHEN
    val actualTraders = prepareDistinctTraders(sparkSession, tradersTestBrokenBatchPath).collect()

    //    THEN: actualTraders should be 0 as 'traders.broken.json' doesnt contain any valid record
    assert(actualTraders.length == 0, "TraderIdValuePair should be equal to 0 as as 'traders.broken.json' doesnt contain any valid record")
  }

  test("Should (re) process TopCategoryTraders per category based on 'bids.simple.json' and 'traders.simple.json' batch data") {
    //    GIVEN
    val givenMaxTraderRank = 20

    val bidsTestBatchPath = getClass.getResource("/bids.simple.json").getPath
    val tradersTestBatchPath = getClass.getResource("/traders.simple.json").getPath

    //    Expected result: ranked by total bids per vehicle category trader id's
    val expectedResult: Seq[RankedCategoryTrader] = Seq(
      //  traderId, category, total value, rank
      RankedCategoryTrader("162d0a2b-7c24-4c71-8f20-2dd69bf6cfa7", "Compact", 2, 1),
      RankedCategoryTrader("0e8b3c0f-62e4-4fcb-8729-8c0362d6345c", "Station", 1, 1),
      RankedCategoryTrader("0e8b3c0f-62e4-4fcb-8729-8c0362d6345c", "Coupe", 1, 1)
    )

    //    WHEN
    val rankedCategoryTraders = reprocessTopCategoryTraders(sparkSession, tradersTestBatchPath, bidsTestBatchPath, givenMaxTraderRank).collect()

    //    THEN: expected and actual data should match
    assert(rankedCategoryTraders.length == expectedResult.length)
    for (actualRankedTrader <- rankedCategoryTraders) {
      assert(expectedResult.contains(actualRankedTrader))
    }
  }

  test("Should (re) process with resolved duplicates TopCategoryTraders per category based on 'bids.duplicated.json' and 'traders.duplicated.json' batch data") {
    //    GIVEN
    val givenMaxTraderRank = 20

    val bidsTestBatchPath = getClass.getResource("/bids.simple.json").getPath
    val tradersTestBatchPath = getClass.getResource("/traders.simple.json").getPath

    val bidsTestDuplicatedBatchPath = getClass.getResource("/bids.duplicated.json").getPath
    val tradersDuplicatedTestBatchPath = getClass.getResource("/traders.duplicated.json").getPath

    //    WHEN
    val deduplicatedRankedCategoryTraders = reprocessTopCategoryTraders(sparkSession, tradersDuplicatedTestBatchPath, bidsTestDuplicatedBatchPath, givenMaxTraderRank).collect()

    //    THEN: comparing to result received from batches which dont have duplicates
    val expectedRankedCategoryTraders = reprocessTopCategoryTraders(sparkSession, tradersTestBatchPath, bidsTestBatchPath, givenMaxTraderRank).collect()
    assert(deduplicatedRankedCategoryTraders.length == expectedRankedCategoryTraders.length)
    for (actualRankedTrader <- deduplicatedRankedCategoryTraders) {
      assert(expectedRankedCategoryTraders.contains(actualRankedTrader))
    }
  }

  test("Should not map broken or null value records from 'traders.broken.json' while reprocessTopCategoryTraders") {
    //    GIVEN
    val givenMaxTraderRank = 20

    val bidsTestBrokenBatchPath = getClass.getResource("/bids.simple.json").getPath
    val tradersTestBrokenBatchPath = getClass.getResource("/traders.broken.json").getPath

    //    WHEN
    val rankedCategoryTraders = reprocessTopCategoryTraders(sparkSession, tradersTestBrokenBatchPath, bidsTestBrokenBatchPath, givenMaxTraderRank).collect()

    //    THEN: idValues should be 0 as 'bids.broken.json' doesnt contain any valid record
    assert(rankedCategoryTraders.length == 0, "RankedCategoryTraders should be equal to 0 as as 'traders.broken.json' doesnt contain any valid record")

  }

}
