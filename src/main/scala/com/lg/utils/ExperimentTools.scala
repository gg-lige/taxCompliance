package com.lg.utils

import java.io.PrintWriter

import com.lg.design.impl.{E_ResultAttr, V_ResultAttr}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD

import scala.collection.Seq
import scala.collection.mutable.HashMap

/**
  * Created by lg on 2018/12/5.
  */
object ExperimentTools {
  def experiment_1(result: Graph[V_ResultAttr, E_ResultAttr]) {
    val scoreAndLabels = result.vertices.map(x => (x._2.new_fz, x._2.wtbz.toDouble)).filter(_._2 != -1.0)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val t = 0.5
    //  for(t<-List(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)) {
    val P_test = scoreAndLabels.filter(_._1 > t).count()
    val N_test = scoreAndLabels.filter(_._1 <= t).count()
    val TP = scoreAndLabels.filter(x => (x._1 > t && x._2 == 1)).count()
    val TN = scoreAndLabels.filter(x => (x._1 <= t && x._2 == 0)).count()
    val FP = scoreAndLabels.filter(x => (x._1 > t && x._2 == 0)).count()
    val FN = scoreAndLabels.filter(x => (x._1 <= t && x._2 == 1)).count()
    // AUC
    val recall = TP.toDouble / (TP + FN)
    val accuracy = (TP + TN).toDouble / (TP + TN + FN + FP)
    val precision = TP.toDouble / (TP + FP)
    val f1 = 2 * precision * recall / (precision + recall)
    val ks = metrics.roc().map(x => (x._2 - x._1)).max
    val bs = scoreAndLabels.map(x => math.pow((x._1 - x._2), 2)).sum().toDouble / scoreAndLabels.count()
    val auc = metrics.areaUnderROC
    val pg = 2 * (new BinaryClassificationMetrics(scoreAndLabels.filter(_._1 < t)).areaUnderROC()) - 1
    println(" P(test):" + P_test + " N(test):" + N_test + " TP:" + TP + " TN:" + TN + " FP:" + FP + " FN:" + FN + " recall:" + recall + " f1:" + f1 + " accuracy:" + accuracy + " precision:" + precision + " ks:" + ks + " bs:" + bs + " AUC:" + auc + " pg:" + pg)
  }

  // i，B，threshold 对 MaxflowCredit 实验结果的影响
  def experiment_2(result: Graph[V_ResultAttr, E_ResultAttr], i: Int, b: Double, threashold: Double, writer: PrintWriter) {
    val scoreAndLabels = result.vertices.map(x => (x._2.new_fz, x._2.wtbz.toDouble))
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val t = 0.5
    //  for(t<-List(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)) {
    val P_test = scoreAndLabels.filter(_._1 > t).count()
    val N_test = scoreAndLabels.filter(_._1 <= t).count()
    val TP = scoreAndLabels.filter(x => (x._1 > t && x._2 == 1)).count()
    val TN = scoreAndLabels.filter(x => (x._1 <= t && x._2 == 0)).count()
    val FP = scoreAndLabels.filter(x => (x._1 > t && x._2 == 0)).count()
    val FN = scoreAndLabels.filter(x => (x._1 <= t && x._2 == 1)).count()
    // AUC
    val recall = TP.toDouble / (TP + FN)
    val accuracy = (TP + TN).toDouble / (TP + TN + FN + FP)
    val precision = TP.toDouble / (TP + FP)
    val f1 = 2 * precision * recall / (precision + recall)
    val ks = metrics.roc().map(x => (x._2 - x._1)).max
    val bs = scoreAndLabels.map(x => math.pow((x._1 - x._2), 2)).sum().toDouble / scoreAndLabels.count()
    val auc = metrics.areaUnderROC
    val pg = 2 * (new BinaryClassificationMetrics(scoreAndLabels.filter(_._1 < t)).areaUnderROC()) - 1
    println(" P(test):" + P_test + " N(test):" + N_test + " TP:" + TP + " TN:" + TN + " FP:" + FP + " FN:" + FN + " recall:" + recall + " f1:" + f1 + " accuracy:" + accuracy + " precision:" + precision + " ks:" + ks + " bs:" + bs + " AUC:" + auc + " pg:" + pg)
    writer.write("\n"+i+"," + b + "," + threashold + "," + P_test + "," + N_test + "," + TP + "," + TN + "," + FP + "," + FN + "," + recall + "," + f1 + "," + accuracy + "," + precision + "," + ks + "," + bs + "," + auc + "," + pg)
  }

  def verify(score:RDD[(VertexId,Double,Double,Int)])= {
    /**
      * id,old_score,new_score,wtbz
      */
    val A = score.map(x=>(x._2,(x._1,x._4))).repartition(1).sortByKey(false).map(x=>(x._2._1,x._1,x._2._2))
    val B = score.map(x => (x._3, (x._1, x._4))).repartition(1).sortByKey(false).map(x => (x._2._1, x._1, x._2._2))
    //按100名波动清况
    var i = 10
    var result = HashMap[VertexId, (Double, Double)]() //节点id,原始命中率,最大流命中率，
    var number = A.count()
    if (number > 1000) {
      number = 1000
    }
    while (i <= number) {
      val PA = A.take(i).filter(_._3 ==1).size / i.toDouble

      val PB = B.take(i).filter(_._3 == 1).size / i.toDouble
      result.put(i, (PA.%(3),  PB.%(3)))
      println(i+","+PA.%(3)+","+ PB.%(3))
      i += 50
    }

    result.toSeq
  }


}
