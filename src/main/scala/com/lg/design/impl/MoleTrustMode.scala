package com.lg.design.impl

import com.lg.consturct.{E_FTGNAttr, V_FTGNAttr}
import org.apache.spark.graphx.Graph

import scala.actors.Actor

/**
  * Created by lg on 2018/12/6.
  */
class MoleTrustMode(step: Int = 1, alpha: Double = 0.5) extends maxflowCreditMode {
  override def description: String

  = s"${this.getClass.getSimpleName}：MoleTrust 模式"

  override def computeGraph(initialGraph: Graph[V_FTGNAttr, E_FTGNAttr]): Graph[V_ResultAttr, E_ResultAttr] = {
    val perprocessGraph = initialGraph.cache()
    val message = perprocessGraph.aggregateMessages[(Double, Int)](e => e.sendToDst(e.attr.capacity * e.srcAttr.compliance, 1),
      ((a, b) => (a._1 + b._1, a._2 + b._2))).mapValues(x => x._1.toDouble / x._2)

    var fixAlreadyGraph = perprocessGraph.outerJoinVertices(message) {
      case (vid, vattr, listMessage) => (vattr.compliance, alpha * listMessage.getOrElse(vattr.compliance) + (1 - alpha) * vattr.compliance, vattr.wtbz)
    }.cache()

    fixAlreadyGraph.mapVertices { case (vid, vattr) => V_ResultAttr(vattr._1, vattr._2, vattr._3) }.mapEdges(e => E_ResultAttr(e.attr.capacity))

  }


}
