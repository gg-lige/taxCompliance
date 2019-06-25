package com.lg.consturct

import com.lg.entity.impl.{E_TESNAttr, V_TESNAttr}
import com.lg.utils.HdfsTools
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import utils.Parameters

import scala.collection.Seq
import scala.reflect.ClassTag


/**
  * Created by lg on 2018/10/22.
  * 构建纳税人显式网络（TESN）
  */
object ConstructTESN extends Serializable {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val hdfsDir: String = Parameters.Dir

    @transient
    val sparkSession = SparkSession.builder().master("spark://192.168.16.1:7077").
      appName("taxCompliance").
      config("spark.jars", "E:\\毕设\\taxCompliance\\out\\artifacts\\taxCompliance_jar\\taxCompliance.jar").
      config("spark.cores.max", "36").
      config("spark.executor.memory", "4g").
     // config("spark.executor.cores", "4").
      config("spark.driver.memory", "6g").
     // config("spark.driver.maxResultSize", "10g").
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

    val tesnFromObject = HdfsTools.getFromObjectFile[V_TESNAttr, E_TESNAttr](sc, s"${hdfsDir}/tesnVertices", s"${hdfsDir}/tesnEdges")
    tesnFromObject.vertices.filter(_._2.isNSR==true).distinct().count()
    tesnFromObject.vertices.filter(_._2.isNSR==false).distinct().count()
    tesnFromObject.edges.map(_.srcId).union(tesnFromObject.edges.map(_.dstId)).distinct().count()
    tesnFromObject.degrees.filter(_._2==0).distinct().count()
    val v1= tesnFromObject.vertices.filter(_._2.isNSR==true).map(_._1).distinct().collect()  //qiye
    val v2= tesnFromObject.vertices.filter(_._2.isNSR==false).map(_._1).distinct().collect()   //ren
    tesnFromObject.edges.filter(_.attr.w_stockholder!=0).filter(e=>v1.contains(e.srcId)&&v2.contains(e.dstId)).distinct().count
    tesnFromObject.edges.filter(_.attr.w_stockholder!=0).filter(e=>v1.contains(e.srcId)&&v1.contains(e.dstId)).distinct().count
    tesnFromObject.edges.filter(_.attr.w_invest!=0).filter(e=>v1.contains(e.srcId)&&v2.contains(e.dstId)).distinct().count
    tesnFromObject.edges.filter(_.attr.w_invest!=0).filter(e=>v1.contains(e.srcId)&&v1.contains(e.dstId)).distinct().count
    tesnFromObject.edges.filter(_.attr.w_legal!=0).distinct().count
    tesnFromObject.edges.filter(_.attr.w_trade!=0).distinct().count
    tesnFromObject.edges.count()
    tesnFromObject.vertices.count()
    tesnFromObject.edges.filter(e=>v2.contains(e.srcId)&& v1.contains(e.dstId)).map(_.dstId).distinct().count

    def localClusteringCoefficient[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = {
      val triCountGraph = g.triangleCount()
      val maxTrisGraph = g.inDegrees.mapValues(srcAttr => srcAttr*(srcAttr-1) / 2.0 )
      triCountGraph.vertices.innerJoin(maxTrisGraph){ (vid, a, b) => if(b == 0) 0 else a / b }
    }
    val a=localClusteringCoefficient(tesnFromObject)
      a.foreach(println)
    println(a.map(_._2).sum() / tesnFromObject.vertices.count())
    val components = tesnFromObject.connectedComponents()
   val b= components.vertices.groupBy(_._2).map(_._2.size)

    b.map(x=>(x,1)).reduceByKey(_+_).sortByKey().map(x=>x._1+","+x._2).repartition(1).saveAsTextFile(s"${hdfsDir}/connectedComTESN.csv")
  }


}
