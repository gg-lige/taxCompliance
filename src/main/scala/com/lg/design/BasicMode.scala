package com.lg.design

import com.lg.utils.{ExperimentTools, HdfsTools}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.Seq
import scala.reflect.ClassTag

/**
  * Created by lg on 2018/11/14.
  *
  * 1.初始化 sc 和 sparkSession
  * 2.抽象方法 getInputGraph 将输入转变为初始图
  * 3.抽象方法 computeGraph 程序计算主体
  * 4.抽象方法 persistGraph 生成的图进行持久化
  * 5.抽象方法 desc 当前程序计算主体的描述方法
  * 6.模板方法 run 运行一次computeGraph 和 persisitGraph
  * 7.普通方法 evaluation 评价指标
  *
  *
  */
abstract class BasicMode[VD1: ClassTag, ED1: ClassTag, VD2: ClassTag, ED2: ClassTag]() extends Serializable {
  @transient
  var sc: SparkContext = _
  @transient
  var sparkSession: SparkSession = _
  @transient
  var initGraph: Graph[VD1, ED1] = _
  @transient
  var adjustedGraph: Graph[VD2, ED2] = _

  initialize()

  def initialize() = {
    val sparkSession = SparkSession.builder().master("spark://192.168.16.1:7077").
      appName("taxCompliance").
      config("spark.jars", "E:\\毕设\\taxCompliance\\out\\artifacts\\taxCompliance_jar\\taxCompliance.jar").
      config("spark.cores.max", "36").
      config("spark.executor.memory", "12g").
      config("spark.driver.memory", "10g").
      config("spark.driver.maxResultSize", "12g").
      getOrCreate()
    sc = sparkSession.sparkContext
    sc.setLogLevel("OFF")
  }

  def getInputGraph(sparkSession: SparkSession): Graph[VD1, ED1]

  def computeGraph(graph: Graph[VD1, ED1]): Graph[VD2, ED2]

  def persistGraph(graph: Graph[VD2, ED2]): Unit

  def persist[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], outputPaths: Seq[String]): Graph[VD, ED] = {
    HdfsTools.saveAsObjectFile(graph, sc, outputPaths(0), outputPaths(1))
    HdfsTools.getFromObjectFile[VD, ED](sc, outputPaths(0), outputPaths(1))
  }

  def description: String

  def run(): Unit = {
    //加入缓存机制，如果 initGraph 已加载则跳过
    if (initGraph == null)
      initGraph = getInputGraph(sparkSession)
    adjustedGraph = computeGraph(initGraph)
    evaluation(adjustedGraph)
  }

  def evaluation(result: Graph[VD2, ED2]) :Unit


}
