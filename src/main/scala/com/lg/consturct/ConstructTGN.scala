package com.lg.consturct

import com.lg.entity.impl.{E_TESNAttr, E_TGNAttr, V_TESNAttr, V_TGNAttr}
import com.lg.utils.HdfsTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3
import utils.Parameters



/**
  * Created by lg on 2018/10/22.
  * 构建纳税人全局网络（TGN）
  */
object ConstructTGN extends Serializable {
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

    if (!HdfsTools.exist(sc, s"${hdfsDir}/cohesionVertices")) {
      val tesnFromObject = HdfsTools.getFromObjectFile[V_TESNAttr, E_TESNAttr](sc, s"${hdfsDir}/tesnVertices", s"${hdfsDir}/tesnEdges")
      //添加控制人亲密度边
      val tesnWithCohesion = TGNTools.addCohesion(tesnFromObject, weight = 0.0).persist()
      println("\n添加互锁边后异构网络: after construct企业:  \n节点数：" + tesnWithCohesion.vertices.count)
      println("边数：" + tesnWithCohesion.edges.count)
      // 节点数：2589914     边数：5733325
      HdfsTools.saveAsObjectFile(tesnWithCohesion, sc, s"${hdfsDir}/cohesionVertices", s"${hdfsDir}/cohesionEdges")
    }


    if (!HdfsTools.exist(sc, s"${hdfsDir}/tgnVertices")) {
      // val tpinFromObject = InputOutputTools.getFromObjectFile[InitVertexAttr, InitEdgeAttr](sc, "/user/lg/maxflowCredit/initVertices", "/user/lg/maxflowCredit/initEdges")
      val tesnWithCohesionFromObject = HdfsTools.getFromObjectFile[V_TESNAttr, E_TESNAttr](sc, s"${hdfsDir}/cohesionVertices", s"${hdfsDir}/cohesionEdges")
      tesnWithCohesionFromObject.degrees.map(_._2).filter(x => (x % 2 != 0)).count
      //抽取所有纳税人子图
      //val tpin_NSR = CreditGraphTools.extractNSR2(tpinFromObject) //不含亲密度边
      val tgn = TGNTools.extractNSR(tesnWithCohesionFromObject) //含亲密度边
      println("\n纳税人全局网络TGN: after construct企业:  \n节点数：" + tgn.vertices.count)
      println("边数：" + tgn.edges.count)
      //节点数：521260    边数：2501885
      HdfsTools.saveAsObjectFile(tgn, sc, s"${hdfsDir}/tgnVertices", s"${hdfsDir}/tgnEdges")

      //将纳税人全局网络中的(节点,纳税人电子档案号)输入至数据库【节点为纳税遵从个体评价的测试集】  513180
      TGNTools.saveVertex(sparkSession, tgn.vertices.filter{case(vid,vattr)=>lang3.StringUtils.isNumeric(vattr.sbh)}.map(v=>(BigDecimal(v._2.sbh).longValue(),v._1)).reduceByKey(_.min(_)).map(v=>(v._2,v._1)) )
    }

  }



}
