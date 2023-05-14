
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

//case class OrdersData(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status:String)

object DataFrames2 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Dataframesex2")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Load Parquet file
  /*val ordersDf = spark.read
  .option("path", "D:/VISHAKHA/BIG DATA COURSE/Week 11/users.parquet")
  .load
  ordersDf.printSchema
  ordersDf.show(false)*/
  
  //Load JSON file
  /*val ordersDf = spark.read
  .format("json")
  .option("path", "D:/VISHAKHA/BIG DATA COURSE/Week 11/players.json")
  .option("mode", "DROPMALFORMED")
  .load
  ordersDf.printSchema()
  ordersDf.show(false)*/
  
  //Load CSV file
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "D:/VISHAKHA/BIG DATA COURSE/Week 11/orders.csv")
  .load()
  
  /*ordersDf.createOrReplaceTempView("orders")
  
  val resultDf = spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count desc")
  
  resultDf.show*/
  
  spark.sql("create database if not exists retail")
  
  ordersDf.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .saveAsTable("retail.orders")
  
  spark.catalog.listTables("retail").show()
  
  /*import spark.implicits._
  val ordersDS = ordersDf.as[OrdersData]
  
  ordersDS.filter(x => x.order_id < 10)
  //ordersDf.filter("order_ids < 10").show()
  //Logger.getLogger(getClass.getName).info("My application has ran successfully")
  ordersDf.printSchema()*/
  
  scala.io.StdIn.readLine()
  spark.stop()
}
