package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

import org.apache.log4j.Logger
import org.apache.log4j.Level

object SimpleSpark extends App {

  def discoverINDs(path: String, inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    //reading all tables, getting all cells per table, read files as strings for better access to single cells
    val tables = inputs.map(file => {
      val fileContent = spark.read.text(path + "/tpch_" + file + ".csv").as[String]

      val tableWithHeader = fileContent.map(line => line.split(";").map(_.trim.replaceAll("\"", "")))
      val header = tableWithHeader.first

      // generate cells of form (value, Set(column name)) and skipping the header
      val tableCells = tableWithHeader
        .filter(_(0) != header(0))
        .flatMap(row => {
          row.zip(header)
        })
        // cell: value, column name
        .map(cell => (cell._1, Set(cell._2)))

      // per table pre-aggregation
      tableCells
        .groupByKey(_._1)
        .reduceGroups((a, b) => (a._1, a._2 ++ b._2))
        .map(_._2)
    })

    //union all cells into one list
    val cells = tables.reduce((a,b) => a.union(b))

    //Aggregation: Grouping by key, adding up the column names
    val attributeSetsPreResult = cells
      .groupByKey(_._1)
      .reduceGroups((a, b) => (a._1, a._2 ++ b._2))

    // attribute sets are hidden in pre-result (in second column, in second tuple value
    val attributeSets = attributeSetsPreResult
      .map(_._2)
      .map(_._2)

    // create distinct attribute sets for better performance
    val distinctAttributeSets = attributeSets.distinct()

    // generate inclusionLists of form (A, S \ A) according to paper
    val inclusionLists = distinctAttributeSets
      .flatMap(set => set.map(p => (p, set - p)))

    // intersect the column names and retrieve only them from groups
    val preINDs = inclusionLists
      .groupByKey(_._1)
      .reduceGroups((a,b) => (a._1, a._2 & b._2))
      .map(_._2)

    // remove empty INDs and sort result, also execute
    val INDs = preINDs
      .filter(_._2.nonEmpty)
      .orderBy(preINDs.columns(0))
      .map(ind => ind._1 + " < " + ind._2.reduce(_ + ", " + _))
      .collect()

    println("OUTPUT")
    INDs.foreach(a => println(_))
  }


  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var path = "TPCH"
    var numWorkers = 4

    if (args.nonEmpty) {
      if (args.length % 2 != 0) {
        println("Provide parameters in pairs!")
        System.exit(0)
      } else {
        args.sliding(2,2).toList.collect {
          case Array("--path", argPath: String) => path = argPath
          case Array("--cores", argCores: String) => numWorkers = argCores.toInt
        }
      }
    }

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark

    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$numWorkers]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", numWorkers * 2) //

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")

    time {discoverINDs(path, inputs, spark)}
  }
}