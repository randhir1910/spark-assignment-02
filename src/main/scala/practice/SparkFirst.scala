//package sparkapps
//
//import org.apache.spark.{SparkConf, SparkContext}
//
//object SparkFirst extends App {
//
//  val conf = new SparkConf().setAppName("spark program").setMaster("local[*]")
//  val sc = new SparkContext(conf)
//  val rdd = sc.parallelize(Array(1,2,3,4,5,6))
//  val res = rdd.collect()
//  println(res)
//  val lines  = sc.textFile("/home/knoldus/Downloads/SparkPractice/files/customer")
//  val counts = lines
//    .flatMap(line => line.split("#"))
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)
//
//  counts.foreach(println)
//    sc.stop()
//}
