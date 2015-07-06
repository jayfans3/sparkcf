package sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by ocean on 7/3/15.
 */
object OceanSql {
  import org.apache.spark.sql._
  def  main (args: Array[String]) {
    import org.apache.spark.sql.SchemaRDD
    val conf =new SparkConf().setAppName("oceansql")
    val context = new SparkContext(conf);
    val sqlcontext =new SQLContext(context);
    case class Rating(u:Int,m:Int,r:Int,t:Int)
    val c=context.textFile("/home/ocean/app/sw/data.dat");
//    val rddrat=context.textFile("hdfs://ocean00:9000/data.dat")
//      .map(_.split("\t")).map(r=>Rating(r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt))
//    rddrat.registerTempTable("rating")
    //    import hiveContext._
//    hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//    hql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
//         sqlcontext.sql("show databases").collect().foreach(println);
     val rate=sqlcontext.createDataFrame(c,Rating.getClass)
    rate.registerTempTable("rate")
    rate.sqlContext.sql("select count(*) from rate").show()
    sqlcontext.sql("select count(*) from userbase").collect().foreach(println)
//    print(rate.count()
  }

}
