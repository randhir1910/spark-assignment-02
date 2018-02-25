package view

import java.util.{Calendar, Date}

import model.{customer, sales}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Time {

  def getYear(time: Long): Int = {
    val date = new Date(time * 1000L)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.YEAR)
  }

  def getMonth(time: Long): Int = {
    val date = new Date(time * 1000L)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH) + 1
  }

  def getDay(time: Long): Int = {
    val date = new Date(time * 1000L)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.DAY_OF_MONTH)
  }
}

object Application extends App {

  val conf = new SparkConf().setAppName("spark-assignment").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val customerData = sc.textFile("/home/knoldus/Downloads/spark-assignment-02/files/customer").map(x => x.split("#"))
  val salesData = sc.textFile("/home/knoldus/Downloads/spark-assignment-02/files/sales").map(x => x.split("#"))
  val customer_record = customerData.map(x => customer(x(0).toInt, x(1), x(2), x(3), x(4), x(5).toInt)).toDF()
  val sales_record = salesData.map(x => sales(Time.getYear(x(0).toInt), Time.getMonth(x(0).toInt), Time.getDay(x(0).toInt), x(1).toInt, x(2).toDouble)).toDF()

  val innerJoin = sales_record.join(customer_record, sales_record("customer_id") === customer_record("customer_id"), "inner")
  innerJoin.select("state", "year", "price").show
  sc.stop()
}
