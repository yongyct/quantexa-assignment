/*
  Quantexa Copyright Statement
 */

package com.quantexa.assignments.transactions

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

/***
  * This Object holds the functions required for the Quantexa coding exercise and an entry point to execute them.
  * Once executed each question is executed in turn printing results to console
  */
  
object TransactionAssignment extends App {

  /***
    * A case class to represent a transaction
    * @param transactionId The transaction Identification String
    * @param accountId The account Identification String
    * @param transactionDay The day of the transaction
    * @param category The category of the transaction
    * @param transactionAmount The transaction value
    */
  case class Transaction(
                          transactionId: String,
                          accountId: String,
                          transactionDay: Int,
                          category: String,
                          transactionAmount: Double)

  case class Question1Result(
                            transactionDay: Int,
                            transactionTotal: Double
                            )

  case class Question2Result(
                            accountId: String,
                            categoryAvgValueMap: Map[String, Double]
                            )

  case class Question3Result(
                            transactionDay: Int,
                            accountId: String,
                            max: Double,
                            avg: Double,
                            aaTotal: Double,
                            ccTotal: Double,
                            ffTotal: Double
                            )

  //The full path to the file to import
  val fileName = getClass.getResource("/transactions.csv").getPath

  //The lines of the CSV file (dropping the first to remove the header)
  //  Source.fromInputStream(getClass.getResourceAsStream("/transactions.csv")).getLines().drop(1)
  val transactionLines: Iterator[String] = Source.fromFile(fileName).getLines().drop(1)

  //Here we split each line up by commas and construct Transactions
  val transactions: List[Transaction] = transactionLines.map { line =>
    val split = line.split(',')
    Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
  }.toList

  /*
   * 
   * END PROVIDED CODE
   * 
   */

  val question1ResultValue = transactions
    .groupBy(_.transactionDay)
    .map( line => Question1Result(line._1, line._2.map(_.transactionAmount).sum) )
    .toSeq
    .sortBy(_.transactionDay)

  val question2ResultValue = transactions
    .groupBy(_.accountId)
    .map({
      line => Question2Result(
        line._1,
        line._2.groupBy(_.category)
          .map({
            categoryGroup => (
              categoryGroup._1,
              categoryGroup._2.map(_.transactionAmount).sum /
              categoryGroup._2.map(_.transactionAmount).size
              )
          })
      )
    })
    .toSeq
    .sortBy(_.accountId)

  val windowSize = 5
  val windowGroups = transactions.groupBy(_.transactionDay)
    .toList
    .sortBy(_._1)
    .sliding(windowSize)
    .zipWithIndex
    .map( x => (x._2 + windowSize + 1, x._1) )
  val question3ResultValue = windowGroups
    .map( x => (x._1, x._2.flatMap( y => y._2 )) )
    .map({
      x => (
        x._1,
        x._2
          .groupBy(_.accountId)
          .map(
            y => Question3Result(
              x._1,
              y._1,
              y._2.map(_.transactionAmount).max,
              y._2.map(_.transactionAmount).sum / y._2.map(_.transactionAmount).size,
              y._2.filter(_.category == "AA").map(_.transactionAmount).sum,
              y._2.filter(_.category == "CC").map(_.transactionAmount).sum,
              y._2.filter(_.category == "FF").map(_.transactionAmount).sum
            )
          )
      )
    })
    .flatMap( x => x._2 )
    .toSeq.sortBy( x => (x.transactionDay, x.accountId) )

  question1ResultValue.foreach(println)
  question2ResultValue.foreach(println)
  question3ResultValue.foreach(println)

}