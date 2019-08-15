package com.accenture.bootcamp.day1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Loader {

  protected def fromResource(resource: String): String = {
    new java.io.File("src/test/resources/" + resource).getCanonicalPath
  }

  def loadNewYearHonours(sc: SparkContext): RDD[String] = {

    // TODO Task #1: Create RDD from file `1918NewYearHonours.txt`
    val filePath = fromResource("1918NewYearHonours.txt")


    val sc_data = sc.textFile(filePath)
    return sc_data

  }

  def loadAustralianTreaties(sc: SparkContext): RDD[String] = {
    // TODO Task #2: Create RDD from file `ListOfAustralianTreaties.txt`
    val filePath = fromResource("ListOfAustralianTreaties.txt")

    val sc_data = sc.textFile(filePath)
    return sc_data
  }


}
