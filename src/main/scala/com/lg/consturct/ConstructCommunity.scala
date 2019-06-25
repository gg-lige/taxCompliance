package com.lg.consturct

import com.lg.consturct.TESNTools.hdfsDir
import com.lg.entity.impl.V_TESNAttr
import com.lg.utils.HdfsTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import utils.{HadoopUtil, Parameters}

/**
  * Created by lg on 2018/12/25.
  */
object ConstructCommunity {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val hdfsDir: String = Parameters.Dir

    @transient
    val sparkSession = SparkSession.builder().master("spark://192.168.16.1:7077").
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
    HadoopUtil.getHadoop.fsRm(s"${hdfsDir}/tax2.labels")
    HadoopUtil.getHadoop.fsRm(s"${hdfsDir}/mathcid2")
    HadoopUtil.getHadoop.fsRm(s"${hdfsDir}/tax2")
    val ftgnGraph = HdfsTools.getFromObjectFile[V_FTGNAttr, E_FTGNAttr](sc, s"${hdfsDir}/ftgnVertices", s"${hdfsDir}/ftgnEdges")
    val startV = ftgnGraph.vertices.filter(_._2.wtbz != (-1)).map(_._1).collect()
    val filterGraph = ftgnGraph.subgraph(vpred = (id, vattr) => (startV.exists(elem => id == elem)))
    val vertexDegree = filterGraph.degrees.persist()
    val graph = Graph(filterGraph.vertices.join(vertexDegree).map(v => (v._1, v._2._1)), filterGraph.edges)

    val ALL_VERTEX = sparkSession.sparkContext.objectFile[(Long, V_TESNAttr)](s"${hdfsDir}/lg_startVertices").repartition(128)
    //oldid,(wtbz,newid,nsrdzdah)
    val matchid = graph.vertices.map(v => (v._1, v._2.wtbz)).repartition(1).zipWithIndex().map(x => (x._1._1, (x._1._2, x._2 + 1))).join(ALL_VERTEX).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._2.sbh)))
    matchid.map(x => (x._2._2, x._1, x._2._3)).repartition(1).sortBy(_._1).repartition(1).saveAsTextFile(s"${hdfsDir}/mathcid2") //newid,oldid,nsrdzdah
    matchid.map(x => x._2._2 + "\t" + x._2._1).repartition(1).saveAsTextFile(s"${hdfsDir}/tax2.labels")
    graph.edges.map(e => (e.srcId, (e.dstId, e.attr.capacity))).join(matchid).map(x => ((x._2._1._1, (x._2._2._2, x._2._1._2)))).join(matchid).map(x => x._2._1._1 + "\t" + x._2._2._2 + "\t" + x._2._1._2).
      repartition(1).saveAsTextFile(s"${hdfsDir}/tax2")

  }
}
