package com.lg.design.impl

import com.lg.consturct.{E_FTGNAttr, V_FTGNAttr}
import org.apache.spark.graphx.{Graph, PartitionID}

import scala.collection.Seq

/**
  * Created by lg on 2018/12/5.
  */
class PeerTrustMode(alpha: Double = 0.5) extends maxflowCreditMode {
  override def description: String

  = s"${this.getClass.getSimpleName}：PeerTrust 模式"

  override def computeGraph(ftgn: Graph[V_FTGNAttr, E_FTGNAttr]): Graph[V_ResultAttr, E_ResultAttr] = {
    val influenceGraph = Graph(ftgn.vertices.mapValues(v => (v.compliance, v.wtbz)), ftgn.edges.mapValues(e => e.attr.capacity))

    val outdegree = influenceGraph.outDegrees
    val indegree = influenceGraph.inDegrees
    val oldfz = influenceGraph.aggregateMessages[Seq[(Double, Double)]](ctx =>
      if (ctx.srcAttr._1 > 0 && ctx.dstAttr._1 > 0) {
        val weight = ctx.attr
        ctx.sendToDst(Seq((ctx.srcAttr._1.toDouble, weight)))
      }, _ ++ _).cache()
    val message = oldfz.leftOuterJoin(outdegree).leftOuterJoin(indegree).map {
      case (vid, (((oldfz, outd), ind))) => (vid, (oldfz, outd.getOrElse(1), ind.getOrElse(1)))
    }

    val fixAlreadyGraph = influenceGraph.outerJoinVertices(message) {
      case (vid, vattr, listMessage) =>
        if (listMessage.isEmpty)
          (vattr._1, vattr._1.toDouble, vattr._2)
        else {
          (vattr._1, AggregateMessage(vattr, listMessage.get, alpha), vattr._2)
        }
    }.cache()
    val max = fixAlreadyGraph.vertices.map(_._2._2).max()
    println(max)
    fixAlreadyGraph.mapVertices { case (vid, (old, newfz, wtbz)) => V_ResultAttr(old, (newfz / max.toDouble * 100).toInt, wtbz) }
      .mapEdges(e => E_ResultAttr(e.attr))
  }

  def AggregateMessage(xyfz: (Double, Int), listMessage: (scala.Seq[(Double, Double)], PartitionID, PartitionID), alpha: Double) = {
    val totalWeight = listMessage._1.map(_._2).sum
    var before = 0D
    listMessage._1.foreach { case (cur_fx, weight) => before += cur_fx * weight / totalWeight }
    val result = alpha * before + (1 - alpha) * listMessage._2 / listMessage._3
    result
  }


  /* val influenceGraph = ftgn
   val outdegree = influenceGraph.outDegrees
   val indegree = influenceGraph.inDegrees
   val oldfz = influenceGraph.aggregateMessages[Seq[(Double, Double)]](ctx =>
     if (ctx.srcAttr.compliance > 0 && ctx.dstAttr.compliance > 0) {
       val weight = ctx.attr.capacity
       ctx.sendToDst(Seq((ctx.srcAttr.compliance, weight)))
     }, _ ++ _).cache()
   val message = oldfz.leftOuterJoin(outdegree).leftOuterJoin(indegree).map {
     case (vid, (((oldfz, outd), ind))) => (vid, (oldfz, outd.getOrElse(1), ind.getOrElse(1)))
   }

   val fixAlreadyGraph = influenceGraph.outerJoinVertices(message) {
     case (vid, vattr, listMessage) =>
       if (listMessage.isEmpty)
         (vattr.compliance, vattr.compliance, vattr.wtbz)
       else {
         (vattr.compliance, AggregateMessage(vattr.compliance, listMessage.get, alpha), vattr.wtbz)
       }
   }.cache()
   val max = fixAlreadyGraph.vertices.map(_._2._2).max()
   println(max)
   fixAlreadyGraph.mapVertices { case (vid, (old, newfz, wtbz)) => V_ResultAttr(old, (newfz / max.toDouble * 100), wtbz) }
     .mapEdges(e => E_ResultAttr(e.attr.capacity))
  }

  def AggregateMessage(xyfz: (PartitionID, Boolean), listMessage: (scala.Seq[(PartitionID, Double)], PartitionID, PartitionID), alpha: Double) = {

   import scala.collection.Seq

   val totalWeight = listMessage._1.map(_._2).sum
   var before = 0D
   listMessage._1.foreach { case (cur_fx, weight) => before += cur_fx * weight / totalWeight }
   val result = alpha * before + (1 - alpha) * listMessage._2 / listMessage._3
   result
  }*/
}
