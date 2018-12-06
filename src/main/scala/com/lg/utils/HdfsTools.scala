package com.lg.utils

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.Parameters

import scala.reflect.ClassTag

/**
  * Created by lg on 2018/11/14.
  */
object HdfsTools {
  // 保存TPIN到HDFS
  def saveAsObjectFile[VD, ED](tpin: Graph[VD, ED], sparkContext: SparkContext,
                               verticesFilePath: String = "", edgesFilePath: String = ""): Unit = {

    checkDirExist(sparkContext, verticesFilePath)
    checkDirExist(sparkContext, edgesFilePath)
    // 对象方式保存顶点集
    tpin.vertices.repartition(128).saveAsObjectFile(verticesFilePath)
    // 对象方式保存边集
    tpin.edges.repartition(128).saveAsObjectFile(edgesFilePath)
  }

  // 保存TPIN到HDFS
  def saveAsTextFile[VD, ED](tpin: Graph[VD, ED], sparkContext: SparkContext,
                             verticesFilePath: String = "", edgesFilePath: String = ""): Unit = {

    checkDirExist(sparkContext, verticesFilePath)
    checkDirExist(sparkContext, edgesFilePath)
    // 对象方式保存顶点集
    tpin.vertices.repartition(1).saveAsTextFile(verticesFilePath)
    // 对象方式保存边集
    tpin.edges.repartition(1).saveAsTextFile(edgesFilePath)
  }

  def checkDirExist(sc: SparkContext, outpath: String) = {
    val hdfs = FileSystem.get(new URI(Parameters.Home), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(outpath), true)
    }
    catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  // 从HDFS获取TPIN
  def getFromObjectFile[VD: ClassTag, ED: ClassTag](sparkContext: SparkContext, verticesFilePath: String = "", edgesFilePath: String = "")
  : Graph[VD, ED] = {
    // 对象方式获取顶点集
    val vertices = sparkContext.objectFile[(VertexId, VD)](verticesFilePath).repartition(128)
    // 对象方式获取边集
    val edges = sparkContext.objectFile[Edge[ED]](edgesFilePath).repartition(128)
    // 构建图
    Graph[VD, ED](vertices, edges)
  }

  def exist(sc: SparkContext, outpath: String) = {
    val hdfs = FileSystem.get(new URI(Parameters.Home), sc.hadoopConfiguration)
    hdfs.exists(new Path(outpath))
  }


}
