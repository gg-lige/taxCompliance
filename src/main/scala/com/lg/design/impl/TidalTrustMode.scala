package com.lg.design.impl

import com.lg.consturct.{E_FTGNAttr, V_FTGNAttr}
import com.lg.entity.{MaxflowEdgeAttr, MaxflowGraph, MaxflowVertexAttr}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.Seq
import scala.collection.mutable.{LinkedHashMap, Set}

/**
  * Created by lg on 2018/12/6.
  */
class TidalTrustMode extends maxflowCreditMode {
  override def description: String

  = s"${this.getClass.getSimpleName}：TidalTrust 模式"


  override def computeGraph(ftgn: Graph[V_FTGNAttr, E_FTGNAttr]): Graph[V_ResultAttr, E_ResultAttr] = ???
}
