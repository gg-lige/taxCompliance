package com.lg.consturct

import com.lg.utils.HdfsTools
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import utils.Parameters

import scala.collection.Seq


/**
  * Created by lg on 2018/10/22.
  * 构建纳税人显式网络（TESN）
  */
object ConstructTESN extends Serializable {
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

    if (!HdfsTools.exist(sc, s"${hdfsDir}/lg_startVertices")) {
      TESNTools.saveE2Oracle_V2HDFS(sparkSession)
      println(s"\nsaveE2Oracle_V2HDFS方法：执行完成")
    } //结束后在数据库进行操作


    if (!HdfsTools.exist(sc, s"${hdfsDir}/tesnVertices")) {
      val tesn = TESNTools.constructTesnMethod(sparkSession)
      println("\n纳税人显式网络TESN :after construct:  \n节点数：" + tesn.vertices.count)
      println("边数：" + tesn.edges.count)
      // 节点数：2589914     边数：3851353
      HdfsTools.saveAsObjectFile(tesn, sc, s"${hdfsDir}/tesnVertices", s"${hdfsDir}/tesnEdges")
    }

  }


}
