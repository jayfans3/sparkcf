package hello

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by ocean on 6/24/15.
 */
object Bayes {

  def main(args: Array[String]) {
    //数据路径
    val data_path = "user/tmp/sample_naive_bayes_data.txt"
    var conf = new SparkConf().setAppName("oceanmap")
    conf.setMaster("local[*]")

    //集群运行模式，读取spark集群的环境变量
    //var conf = new SparkConf().setAppName("Moive Recommendation")

    val sc = new SparkContext(conf)
    //读取数据，转换成LabeledPoint类型
    val examples = sc.textFile(data_path).map { line =>
      val items = line.split(',')
      val label = items(0).toDouble
      val value = items(1).split(' ').toArray.map(f => f.toDouble)
      LabeledPoint(label, Vectors.dense(value))
    }
    examples.cache()
    //样本划分，80%训练，20%测试
    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0)
    val test = splits(1)
    val numTraining = training.count()
    val numTest = test.count()
    println(s"numTraining = $numTraining, numTest = $numTest.")
    //样本训练，生成分类模型
    val model = new NaiveBayes().setLambda(1.0).run(training)
    //根据分类模型，对测试数据进行测试，计算测试数据的正常率
    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))
    val accuracy = predictionAndLabel.filter(x => x._1 == x._2).count().toDouble / numTest
    println(s"Test accuracy = $accuracy." + s"$accuracy")

  }


}
