package com.lg.consturct

import com.lg.entity.impl.{E_TGNAttr, V_TGNAttr}
import com.lg.utils.HdfsTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import utils.Parameters

/**
  * Created by lg on 2018/12/5.
  */
case class V_FTGNAttr(compliance: Double, wtbz: Int) extends Serializable

case class E_FTGNAttr(capacity: Double) extends Serializable

object ConstructFTGN {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val hdfsDir: String = Parameters.Dir

    @transient
    val sparkSession = SparkSession.builder().master("spark://192.168.16.187:7077").
      appName("taxCompliance").
      config("spark.jars", "E:\\毕设\\taxCompliance\\out\\artifacts\\taxCompliance_jar\\taxCompliance.jar").
      config("spark.cores.max", "36").
      config("spark.executor.memory", "12g").
      config("spark.executor.cores", "4").
      config("spark.driver.memory", "10g").
      config("spark.driver.maxResultSize", "12g").
      getOrCreate()

    @transient
    val sc = sparkSession.sparkContext
    sc.setLogLevel("OFF")

    //修正图上的边权值,并提取点度>0的节点（信息融合等原理）
    if (!HdfsTools.exist(sc, s"${hdfsDir}/ftgnVertices")) {
      val tgn = HdfsTools.getFromObjectFile[V_TGNAttr, E_TGNAttr](sc, s"${hdfsDir}/tgnVertices", s"${hdfsDir}/tgnEdges")
      val gt_compliance = sparkSession.sparkContext.textFile(s"${hdfsDir}/compliance").filter(!_.contains("nsrdzdah")).
        map(_.split(",")).filter(_.length == 3).map(row => (row(0), row(1).toDouble, row(2).toDouble.toInt))
      val edgetemp = tgn.mapEdges(e => E_TGNAttr.fusion(e)).edges.mapValues(x => E_FTGNAttr(x.attr._1))
      val vertextemp = tgn.vertices.map(v => (v._2.sbh, (v._1, v._2.compliance, v._2.wtbz))).leftOuterJoin(gt_compliance.map(v => (v._1, (v._2, v._3)))).map {
        x => {
          if (!x._2._2.isEmpty) {
            (x._2._1._1, V_FTGNAttr(x._2._2.get._1, x._2._2.get._2))
          }
          else
            (x._2._1._1, V_FTGNAttr(x._2._1._2, x._2._1._3))
        }
      }
      val fixEdgeWeightGraph = Graph(vertextemp, edgetemp).persist()
      println("\n修正边权值fixEdgeWeightGraph:  \n节点数：" + fixEdgeWeightGraph.vertices.count)
      println("边数：" + fixEdgeWeightGraph.edges.count)
      println("有问题：" + fixEdgeWeightGraph.vertices.filter(_._2.wtbz == 1).count)
      println("无问题：" + fixEdgeWeightGraph.vertices.filter(_._2.wtbz == 0).count)
      println("未标记：" + fixEdgeWeightGraph.vertices.filter(_._2.wtbz == -1).count)
      //节点数：521260       边数：2501885
      //有问题：2462
      //无问题：147
      //未标记：518651
      HdfsTools.saveAsObjectFile(fixEdgeWeightGraph, sc, s"${hdfsDir}/ftgnVertices", s"${hdfsDir}/ftgnEdges")
    }


  }
}
