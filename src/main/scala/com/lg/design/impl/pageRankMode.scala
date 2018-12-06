package com.lg.design.impl

import com.lg.consturct.{E_FTGNAttr, V_FTGNAttr}
import org.apache.spark.graphx._

import scala.collection.Seq

/**
  * Created by lg on 2018/12/5.
  */
class pageRankMode(var resetProb: Double = 0.15, var tol: Double = 1) extends maxflowCreditMode {
  override def description: String

  = s"${this.getClass.getSimpleName}：pageRank 模式"

  override def computeGraph(ftgn: Graph[V_FTGNAttr, E_FTGNAttr]): Graph[V_ResultAttr, E_ResultAttr] = {
    val graph = ftgn
    val degree = graph.aggregateMessages[Seq[Double]](ctx => {
      ctx.sendToSrc(Seq(ctx.attr.capacity))
    }, _ ++ _).map(e => (e._1, e._2.sum))
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph = graph
      .outerJoinVertices(degree) {
        case (vid, old, degree) => if (degree.isEmpty) (old.compliance, 1D) else (old.compliance, degree.get)
      }
      .mapTriplets(e => e.attr.capacity / e.srcAttr._2)
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices { case (id, (old, degree)) => (old.toDouble, 0.0) }
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      if (msgSum == -1D)
        (oldPR, tol + 1)
      else if (msgSum > 100) {
        val newPR = oldPR * resetProb + (1.0 - resetProb) * 100
        (newPR, newPR - oldPR)
      } else {
        val newPR = oldPR * resetProb + (1.0 - resetProb) * msgSum
        (newPR, newPR - oldPR)
      }
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = -1D // resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp =
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)

    val result = Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out, maxIterations = 4)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
    val max = result.vertices.map(_._2).max()
    result.mapVertices { case (vid, fz) => fz / max * 100 }.
      outerJoinVertices(graph.vertices) { case (vid, newf, opt) => V_ResultAttr(opt.get.compliance, newf.toDouble, opt.get.wtbz) }.
      mapEdges(e => E_ResultAttr(e.attr))
  }

}
