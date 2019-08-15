package com.accenture.bootcamp

import org.apache.spark.{SparkConf, SparkContext}
import com.accenture.bootcamp.day1.Loader
import com.accenture.bootcamp.day1.Tokenizer
import com.accenture.bootcamp.day1.Tokenizer.{tokenize, wordClassifier}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.ListMap

/**
 * @author ${user.name}
 */
//object App extends Tasks {
 object App{

  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)

  def ex_task_9(): Double ={

    return 12.23
  }


  def main(args : Array[String]) {


    println( "Hello World!" )
    println("concat arguments = " + foo(args))

    val sparkSession = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext




    var ddr_string_1 = Loader.loadAustralianTreaties(sc)
    var tokenize_var_1 = Tokenizer.tokenize(ddr_string_1)
    var countWords_var_1 = Tokenizer.countWords(tokenize_var_1)

    var ddr_string_2 = Loader.loadNewYearHonours(sc)
    var tokenize_var_2 = Tokenizer.tokenize(ddr_string_2)
    var countWords_var_2 = Tokenizer.countWords(tokenize_var_2)

////    Task 3
//    var tmp_text_3 = "Bilateral treaties prior to federation signed by the British Empire, adopted by Australia, and active on or after federation:\n\n1842"
//    var test_3 = Tokenizer.words(tmp_text_3)
//    println("Task 3:  ")
//    println("1842 – Treaty 5 March 1856)[5]")
//    for (e <- test_3) println(e)
//
//    println("Task_5 - loadAustralianTreaties:  " + countWords_var_1)
//
//    val countWords_var_FINAL = countWords_var_1 + countWords_var_2
//    println("Task_6 - together: " + countWords_var_FINAL)
//
////    Task 7
    val countNumWords_var_1 = Tokenizer.numbers(ddr_string_1)
//    println("Task 7:  ")
//    for (e <- countNumWords_var_1) println(e)
//
////    Task 8
//    val unique_val_1 = countNumWords_var_1.distinct().count()
//    println("Task 8  :" + unique_val_1)


//    //    Task 9
//    val average_var_1 : Double = countNumWords_var_1.sum() / countNumWords_var_1.count()
//    println("Task 9  (average):" + average_var_1)

    //    Task 10

//    var word_accurancies = Tokenizer.wordFrequency(ddr_string_1)
////    word_accurancies.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))

//    //    Task 11
//    var map_sort_by_value = word_accurancies.sortBy(_._2, false).take(10).map(_._1)
//    println("Task 11:   ")
//    for (e <- map_sort_by_value) println(e)

    //    Task 12


    println("Task 12:   ")
    val arr = Array(".", ".1", ".a", ":b", "0a", "1b", "1234567890", "mama", "maolun", "akiko", "sayonara", "nymphs", "wynds")

    for (e <- arr) {
      val wordStats_val = Tokenizer.wordStats(e)
//      wordStats_val.productIterator.foreach(println)
      println(Tokenizer.wordStatsClassifier(wordStats_val))
    }

    //    Task 13

//    var task_13_word_list = tokenize(ddr_string_2).map(word => (wordClassifier(word), 1.toLong))
//      .reduceByKey(_+_).collect().toMap
//    println("Task 13:     ")
//    task_13_word_list.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))
  }






}
