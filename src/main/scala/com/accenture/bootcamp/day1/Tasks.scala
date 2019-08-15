package com.accenture.bootcamp.day1


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//import com.accenture.bootcamp.day1.Tokenizer._
//import com.accenture.bootcamp.day1.Loader._


trait Tasks {

  def sc: SparkContext

//  def classifier

  val newYearHonours: RDD[String] = Loader.loadNewYearHonours(sc)
  val australianTreaties: RDD[String] = Loader.loadAustralianTreaties(sc)

  var ddr_string_1 = Loader.loadNewYearHonours(sc)
  var tokenize_var_1 = Tokenizer.tokenize(ddr_string_1)
  var countWords_var_1 = Tokenizer.countWords(tokenize_var_1)

  var ddr_string_2 = Loader.loadAustralianTreaties(sc)
  var tokenize_var_2 = Tokenizer.tokenize(ddr_string_2)
  var countWords_var_2 = Tokenizer.countWords(tokenize_var_2)


  /**
    * Task #5: How many words are in ListOfAustralianTreaties.txt?
    * Hint: use countWords() to count amount of words
    *
    * @return amount of words
    */
  def task5(): Long = {
    // TODO Task #5: How many words are in ListOfAustralianTreaties.txt?

    return countWords_var_1
//    ???
  }

  /**
    * Task #6: How many words are in both .txt files?
    * Hint: use countWords() to count amount of words
    *
    * @return amount of words in both .txt files
    */
  def task6(): Long = {
    // TODO Task #6: How many words are in both .txt files?
    val countWords_var_FINAL = countWords_var_1 + countWords_var_2
    return countWords_var_FINAL
  }

  /**
    * Task #8: How many unique numbers are in ListOfAustralianTreaties.txt? 
    *
    * @return
    */
  def task8(): Long = {
    // TODO Task #8: How many unique numbers are in ListOfAustralianTreaties.txt? 
    val unique_val_1 = Tokenizer.numbers(ddr_string_2).distinct().count()
    return unique_val_1
  }

  /**
    * Task #9: Calculate average of all numbers in ListOfAustralianTreaties.txt? 
    * i.e. string "1842 – Treaty 5 March 1856)[5]» has average 927
    *
    * @return average value for ListOfAustralianTreaties.txt
    */
  def task9(): Double = {
    // TODO Task #9: Calculate average of all numbers in ListOfAustralianTreaties.txt? 
    val tok_num = Tokenizer.numbers(ddr_string_2)
    val average_var_1 : Double = tok_num.sum() / tok_num.count()
    return average_var_1
  }

  /**
    * Task #11: What are 10 most frequent symbols in ListOfAustralianTreaties.txt?
    * Hint: use wordFrequency()
    * Hint: the result should be sorted in descending way
    *
    * @return
    */
  def task11: Seq[String] = {
    // TODO Task #11: What are 10 most frequent symbols in ListOfAustralianTreaties.txt?
    val get_data = Tokenizer.wordFrequency(ddr_string_2)
    var get_sorted_data = get_data.sortBy(_._2, false).take(10).map(_._1)
    return get_sorted_data
//    ???
  }

  /**
    * Task #13: How many elements there are in each group in ListOfAustralianTreaties.txt
    * Hint: use Tokenizer.classify()
    *
    * @return
    */
  def task13: Map[String, Long] = {
    // TODO Task #13: How many elements there are in each group in ListOfAustralianTreaties.txt
    var word_list = Tokenizer.tokenize(ddr_string_2).map(word => (Tokenizer.wordClassifier(word), 1.toLong))
      .reduceByKey(_+_).collect().toMap
    word_list
//    ???
  }

  /**
    * Task #14: Print samples of each group with A, B, C, D values from ListOfAustralianTreaties.txt?
    * Hint: use Tokenizer.wordClassifier()
    * @return
    */
  def task14: RDD[(String, (Int, Int, Int, Int))] = {
    // TODO Task #14: Print samples of each group with A, B, C, D values from ListOfAustralianTreaties.txt?
    Tokenizer.tokenize(australianTreaties).map(word => (Tokenizer
      .wordClassifier(word).toString, Tokenizer.wordStats(word)))
  }

}
