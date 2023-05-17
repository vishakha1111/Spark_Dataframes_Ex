import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object dataframe_big_file {
  
  case class Logging(level:String, datetime:String)
  
  def mapper(line:String): Logging = {
    val fields = line.split(',')
    
    val logging:Logging = Logging(fields(0), fields(1))
    return logging
  }
  
 
def main(args: Array[String]){
     
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession
    .builder
    .appName("SparkSQL")
    .master("local[*]")
    .getOrCreate()
  
  import spark.implicits._
  
  /*
  	val mylist = List("WARN,2016-12-31 04:19:32",
    "FATAL,2016-12-31 03:22:34",
    "WARN,2016-12-31 03:21:21",
    "INFO,2015-4-21 14:32:21",
    "FATAL, 2015-4-21 19:23:20")
    
   val rdd1 = spark.sparkContext.parallelize(mylist)
   val rdd2 = rdd1.map(mapper)
   val df1 = rdd2.toDF()
   //df1.show
   
   df1.createOrReplaceTempView("logging_table")
   //spark.sql("select * from logging_table").show
   
   //spark.sql("select level,count(datetime) from logging_table group by level order by level")
   //.show(false)
   
   val df2 = spark.sql("select level, date_format(datetime, 'MMMM') as Month from logging_table")
   
   df2.createOrReplaceTempView("new_logging_table")
   
   spark.sql("select level, month, count(1) from new_logging_table group by level, month").show
   */
   val df3 = spark.read
   .option("header", true)
   .csv("D:/VISHAKHA/BIG DATA COURSE/Week 12/biglog.txt")
   
   //df3.show
   
   df3.createOrReplaceTempView("my_new_logging_table")
   
   val results = spark.sql("""select level, date_format(datetime, 'MMMM')
   as month, count(1) as total from my_new_logging_table group by level, month""")
  
   //results.createOrReplaceTempView("Results_table")
   
   val result1 = spark.sql("""select level, date_format(datetime, 'MMMM')as month, 
   cast(first(date_format(datetime, 'M')) as int) as monthnum,
   count(1) as total from my_new_logging_table 
   group by level, month order by monthnum, level""")
   
   val result2 = result1.drop("monthnum").show(60)
   
}
}
