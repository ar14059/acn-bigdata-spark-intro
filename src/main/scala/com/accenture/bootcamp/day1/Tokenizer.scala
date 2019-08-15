package com.accenture.bootcamp.day1

import org.apache.spark.rdd.RDD

import scala.util.control.Exception.allCatch

import scala.collection.immutable.ListMap

object Tokenizer {

  type Word = String
  type Classifier = String
  type Amount = Long
  type WordStats = (Int, Int, Int, Int)

  /**
    * Task #3: Tokenize (split string into words) 
    * String "1842 – Treaty 5 March 1856)[5]" should consists of following words: 1842,Treaty,5,March,1856,5
    * @param line any string
    * @return
    */
  def words(line: String): Array[Word] = {
    // TODO Task #3: Tokenize (split string into words) 
//    val line_split = line.replaceAll("([^A-Za-z0-9\\s])", " ")
//      .replaceAll(" +", " ").split(" ")
    val line_split = line.replaceAll("([^A-Za-z0-9\\s])", " ")
      .replaceAll("[\\t\\n\\r]+"," ")
      .replaceAll(" +", " ").split(" ")

    return line_split
  }

  def tokenize(rdd: RDD[String]): RDD[Word] = rdd.flatMap(words)

  /**
    * Task #4: Count words in RDD
    * Given RDD[String]. You need tokenize it using method words() and count words
    * @param rdd input RDD
    * @return word count
    */
  def countWords(rdd: RDD[Word]): Amount = {
    // TODO Task #4: Count words in RDD
//    var counts = tokenize(rdd).map(word => (word,1)).reduceByKey(_+_).count()
    var counts = tokenize(rdd).count()
    return counts



    ???
  }

  /**
    * Task #7: Transform RDD so that it should contain numbers only
    * i.e. string "1842 – Treaty 5 March 1856)[5]" should consists of following numbers:
    * 1842, 5, 1856, 5
    *
    * @param text RDD with text
    * @return RDD with numbers only
    */
  def numbers(text: RDD[Word]): RDD[Long] = {
    // TODO Task #7: Transform RDD so that it should contain numbers only
    var long_rdd = tokenize(text).filter(_.matches("[0-9]+")).map(_.toLong)
    return long_rdd
  }

  /**
    * Task #10: Get word occurrences
    * Count how often each word repeats
    *
    * @return
    */
//  def wordFrequency(words: RDD[Word]):RDD[Any] = {
  def wordFrequency(words: RDD[Word]):RDD[(Word, Int)] = {
    // TODO Task #10: Get word occurrences
    // TODO Task #10.1: Replace output type RDD[Any] with correct one
    var wordFrequency_var = tokenize(words).map(word => (word.toLowerCase(),1)).reduceByKey(_+_)
    return wordFrequency_var
  }

  /**
    * Task #12a: Gather word stats by 4 criteria such as:
    *   A = Number of digits
    *   B = Number of vowels
    *   C = Number of consonants
    *   D = Number of other symbols
    *
    * @param word word need to be classified
    * @return
    */
  def wordStats(word: Word): WordStats = {
    // TODO Task #12a: Gather word stats by 4 criteria

    var regex_A_num_of_digits = "[^0-9]"
    var regex_A_filter = word.toLowerCase().replaceAll(regex_A_num_of_digits,"")
    var regex_B_num_of_vowels = "[^aeiuoyāēīū]"
    var regex_B_filter = word.toLowerCase().replaceAll(regex_B_num_of_vowels,"")
    var regex_C_num_of_consonants = "[^a-z]"
    var regex_C_filter = word.toLowerCase().replaceAll(regex_C_num_of_consonants,"")
      .replaceAll("[aeiuoyāēīū]", "")
    var regex_D_others = "[a-z0-9āēīū]"
    var regex_D_filter = word.toLowerCase().replaceAll(regex_D_others,"")


    return (regex_A_filter.length, regex_B_filter.length, regex_C_filter.length, regex_D_filter.length)
  }

  /**
    * Task #12b: Classify word statistics into 5 groups such as:
    *   Group 0: where D > 0 or A > 0 and B+C >0, name it “thrash”
    *   Group 1: where A > 0, name it “numbers”
    *   Group 2: where B == C, name it “balanced_words”
    *   Group 3: where B > C, name it “singing_words”
    *   Group 4: others, name it “grunting_words”
    * Where:
    *   A = Number of digits
    *   B = Number of vowels
    *   C = Number of consonants
    *   D = Number of other symbols
    *
    * @param wordStats word statistics (A, B, C, D)
    * @return
    */
  def wordStatsClassifier(wordStats: WordStats): Classifier = {
    // TODO Task #12b: Classify word by

    if(wordStats._4 > 0 || wordStats._1 > 0 && wordStats._2 + wordStats._3 > 0){
      "thrash"
    }else if(wordStats._1 > 0){
      "numbers"
    }else if(wordStats._2 == wordStats._3){
      "balanced_words"
    }else if(wordStats._2 > wordStats._3){
      "singing_words"
    }else{
      "grunting_words"
    }

  }

  def wordClassifier(word: Word): Classifier = {
    wordStatsClassifier(wordStats(word))
  }

  /**
    * Task #13a: How many elements there are in each group
    * Hint: Use wordClassifier() to implement this method
    *
    * @param words words for classification
    * @return classification
    */
  def classify(words: RDD[Word]): Map[Classifier, Amount] = {
    // TODO Task #13a: How many elements there are in each group
    var word_list = words.map(word => (wordClassifier(word), 1.toLong))
      .reduceByKey(_+_).collect().toMap
    word_list
  }

}
