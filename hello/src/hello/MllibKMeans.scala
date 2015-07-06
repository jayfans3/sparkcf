package hello

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext

/**
 * Created by ocean on 6/30/15.
 */
object MllibKMeans {
  def main(args: Array[String]) {
    //数据路径
    var conf = new SparkConf().setAppName("oceanmap")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data_path = "user/tmp/kmeans_data.txt"
    val data = sc.textFile(data_path)
    val examples = data.map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()
    val numExamples = examples.count()
    println(s"numExamples = $numExamples.")
    //2建立模型
    val k = 2
    val maxIterations = 20
    val runs = 2
    val initializationMode = "k-means||"
    val model = KMeans.train(examples, k, maxIterations, runs, initializationMode)
    //3计算测试误差
    val cost = model.computeCost(examples)
    println(s"Total cost = $cost.")
  }


}
