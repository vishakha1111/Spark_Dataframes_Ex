import java.security.Timestamp

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class OrdersData(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status:String)

object DataFramesExample extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first application")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //Load Parquet file
  val ordersDf = spark.read
  .option("path", "D:/VISHAKHA/BIG DATA COURSE/Week 11/users.parquet")
  .load
  ordersDf.printSchema
  ordersDf.show(false)
  
  //Load JSON file
  /*val ordersDf = spark.read
  .format("json")
  .option("path", "D:/VISHAKHA/BIG DATA COURSE/Week 11/players.json")
  .option("mode", "DROPMALFORMED")
  .load
  ordersDf.printSchema()
  ordersDf.show(false)*/
  
  //Load CSV file
  /*val ordersDf = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("D:/VISHAKHA/BIG DATA COURSE/Week 11/orders.csv")
  
  import spark.implicits._
  val ordersDS = ordersDf.as[OrdersData]
  
  ordersDS.filter(x => x.order_id < 10)
  //ordersDf.filter("order_ids < 10").show()
  //Logger.getLogger(getClass.getName).info("My application has ran successfully")
  ordersDf.printSchema()*/
  
  scala.io.StdIn.readLine()
  spark.stop()
}
