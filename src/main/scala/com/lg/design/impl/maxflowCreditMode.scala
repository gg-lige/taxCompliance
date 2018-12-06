package com.lg.design.impl

import java.io.{File, PrintWriter}

import com.lg.consturct.{E_FTGNAttr, V_FTGNAttr}
import com.lg.design.BasicMode
import com.lg.entity.{MaxflowEdgeAttr, MaxflowGraph, MaxflowVertexAttr}
import com.lg.utils.{ExperimentTools, HdfsTools}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.Parameters

import scala.collection.mutable.{HashMap, LinkedHashMap, Set}

/**
  * Created by lg on 2018/11/14.
  */


case class V_ResultAttr(old_fz: Double, new_fz: Double, wtbz: Int) extends Serializable

case class E_ResultAttr(influence: Double) extends Serializable

object maxflowCreditMode {

  def main(args: Array[String]): Unit = {
    val hdfsDir: String = Parameters.Dir

    val outputVerifyMode = true
    if (outputVerifyMode == true) {
      // i，B，threshold 对 MaxflowCredit 实验结果的影响
      val writer = new PrintWriter(new File("E:\\毕设\\taxCompliance\\result\\ensembleMaxflow.csv"))
      writer.write("b,threashold,P_test,N_test,TP,TN,FP,FN,recall,f1,accuracy,precision,ks,bs,auc,pg")
      for (i <- List(3)) { //gain 衰减函数  : 1,2,3,4
        val Bs = List(0.7) //B 分配百分比  ： 0.1，0.3，0.5，0.7，0.9
      val threasholds = List(0.7) //threasholds 阈值分配百分比 ： 0D, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9
        for (b <- Bs) {
          for (threashold <- threasholds) {
            val instance = new maxflowCreditMode(false, false, i, b, threashold)
            instance.sc.setLogLevel("OFF")
            val ftgn = instance.getInputGraph(instance.sparkSession)
            val result = instance.computeGraph(ftgn)
            ExperimentTools.experiment_2(result, i, b, threashold, writer)
          }
        }
      }
      writer.close()
    }
    if (outputVerifyMode == false) {
      //上述最优 i,B,threshold 下 TOP1-TOP300 命中率
      val besti = 0
      val bestb = 0d
      val bestthreashold = 0d
      val instance = new maxflowCreditMode(false, false, besti, bestb, bestthreashold)
      val ftgn = instance.getInputGraph(instance.sparkSession)
      val result = instance.computeGraph(ftgn)
      result.vertices.map(v => (v._2.new_fz, v._1, v._2.old_fz, v._2.wtbz)).repartition(1).sortBy(_._1).map { line =>
        (line._2, line._3, line._1, line._4)
      }.repartition(1).saveAsTextFile(s"${hdfsDir}/TOP300.csv")
    }

  }

}

class maxflowCreditMode(val forceReConstruct: Boolean = false,
                        val forceReAdjust: Boolean = false, val i: Int = 3, val b: Double = 0D, threashold: Double = 0D) extends BasicMode[V_FTGNAttr, E_FTGNAttr, V_ResultAttr, E_ResultAttr] {
  val hdfsDir: String = Parameters.Dir

  override def getInputGraph(sparkSession: SparkSession): Graph[V_FTGNAttr, E_FTGNAttr] = {
    HdfsTools.getFromObjectFile[V_FTGNAttr, E_FTGNAttr](sc, s"${hdfsDir}/ftgnVertices", s"${hdfsDir}/ftgnEdges")
  }


  override def computeGraph(ftgn: Graph[V_FTGNAttr, E_FTGNAttr]): Graph[V_ResultAttr, E_ResultAttr] = {

    //求最大流子图：各节点向外扩展3步，每步选择邻近的前selectTopN个权值较大的点向外扩展，得到RDD（节点，所属子图）,同时选择中心节点至少含有一个入度,
    val extendPair = extendSubgraph(ftgn, 6)
    var oneIndegreeExtendPair = extendPair.filter(x => x._2.getAllEdge().map(_.dst.id).contains(x._1))
    val maxflowSubExtendPair = ftgn.vertices.filter(_._2.wtbz != -1).map(x => (x._1, x._2.compliance)).leftOuterJoin(oneIndegreeExtendPair).map(x => (x._1, x._2._2.getOrElse {
      val a = new MaxflowGraph()
      a.getGraph().put(MaxflowVertexAttr(x._1, x._2._1), List[MaxflowEdgeAttr]())
      a
    })).cache()
   // println("最大子图规模：" + maxflowSubExtendPair.map(_._2.getAllEdge().size).max)
  //  println("最少有一个入度节点的个数：" + oneIndegreeExtendPair.count())
    val maxflowCredit = run3(maxflowSubExtendPair, b, threashold, i) //  （中心节点ID，(1-β)后面，最大流得分，周边各节点流向中间的流量列表）
    val subftgn = ftgn.subgraph(vpred = (vid, vattr) => vattr.wtbz != -1)
    val vertex = subftgn.vertices.leftOuterJoin(maxflowCredit.map(x => (x._1, (x._3)))).map(x => (x._1, V_ResultAttr(x._2._1.compliance, x._2._2.getOrElse(0D), x._2._1.wtbz)))
    val edge = subftgn.edges.map(e => Edge(e.srcId, e.dstId, E_ResultAttr(e.attr.capacity)))
    Graph(vertex, edge)
  }

  override def persistGraph(graph: Graph[V_ResultAttr, E_ResultAttr]): Unit

  = {


  }

  override def description: String

  = s"${this.getClass.getSimpleName}：广义最大流计算方式"

  override def evaluation(result: Graph[V_ResultAttr, E_ResultAttr]) = {
    ExperimentTools.experiment_1(result)
  }


  /**
    * 节点上存储子图【选用这个】
    */
  def extendSubgraph(graph: Graph[V_FTGNAttr, E_FTGNAttr], selectTopN: Int): RDD[(VertexId, MaxflowGraph)] = {

    val fixEdgeWeightGraph = Graph(graph.vertices.mapValues(v => (v.compliance)), graph.edges.mapValues(_.attr.capacity))
    val neighbor = fixEdgeWeightGraph.aggregateMessages[Set[(VertexId, VertexId, Double, Double, Boolean)]](triple => {
      //发送的消息为：节点编号，(源终节点编号),边权重，节点属性，向源|终点发
      if (triple.srcId != triple.dstId) { //去环
        //   triple.sendToSrc(Set((triple.dstId, triple.srcId, triple.attr, triple.dstAttr, false)))
        triple.sendToDst(Set((triple.srcId, triple.dstId, triple.attr, triple.srcAttr, true))) //只向终点发
      }
    }, _ ++ _)


    val neighborVertex1 = neighbor.leftOuterJoin(graph.vertices.filter(_._2.wtbz != -1)).map(x => (x._1, x._2._1, x._2._2.getOrElse(V_FTGNAttr(0d, -1)).wtbz)).map { case (vid, vattr, label) =>
      //  val neighborVertex1 = neighbor.leftOuterJoin(maxflowSubGraph).map(x => (x._1, x._2._1, x._2._2.getOrElse((-1, 2))._2)).map { case (vid, vattr, label) =>
      val newattr = vattr.toSeq.filter(_._5 == true)


      val badNum = newattr.filter(_._4 > 0.5).size
      val badNodes = newattr.filter(_._4 > 0.5).sortBy(_._3)(Ordering[Double].reverse)

      val goodNum = newattr.filter(_._4 <= 0.5).size
      val goodNodes = newattr.filter(_._4 <= 0.5).sortBy(_._3)(Ordering[Double].reverse)

      val smallNum = badNum.min(goodNum)
      if (label == 1) {
        if (vattr.size > selectTopN)
          (vid, badNodes.slice(0, selectTopN))
        else
          (vid, badNodes)
      }
      else if (label == 0) {
        if (vattr.size > selectTopN)
          (vid, goodNodes.slice(0, selectTopN))
        else
          (vid, goodNodes)

      }
      else {
        if (badNum <= selectTopN / 2 && goodNum <= selectTopN / 2)
          (vid, newattr)
        else if (badNum < selectTopN / 2 && goodNum > selectTopN / 2)
          (vid, badNodes.slice(0, badNum).union(goodNodes.slice(0, selectTopN / 2)).toSet)
        else if (badNum > selectTopN / 2 && goodNum < selectTopN / 2)
          (vid, badNodes.slice(0, selectTopN / 2).union(goodNodes.slice(0, goodNum)).toSet)
        else
          (vid, badNodes.slice(0, selectTopN / 2).union(goodNodes.slice(0, selectTopN / 2)).toSet)
      }


    }.cache()


    val neighborVertex = neighbor.map { case (vid, vattr) =>
      if (vattr.size > selectTopN)
        (vid, vattr.toSeq.sortBy(_._3)(Ordering[Double].reverse).slice(0, selectTopN).toSet)
      else
        (vid, vattr)
    }.cache()

    //一层邻居 （社团，不包含节点自身）
    // val neighborPair1 = neighborVertex.map(x => (x._1, x._2)).flatMap(v1 => v1._2.map(v2 => (v1._1, v2))).distinct //指向及指出中心节点
    val neighborPair1 = neighborVertex1.map(x => (x._1, x._2)).flatMap(v1 => v1._2.map(v2 => (v1._1, v2))).distinct //只指向中心节点
    //二层邻居
    val neighborPair2 = neighborPair1.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //三层邻居
    val neighborPair3 = neighborPair2.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //四层邻居
    val neighborPair4 = neighborPair3.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //五层邻居
    val neighborPair5 = neighborPair4.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //六层邻居
    val neighborPair6 = neighborPair5.map(x => (x._2._1, x._1)).join(neighborPair1).map(x => (x._2._1, x._2._2)).distinct
    //后面构子图时为中心节点添加边权重，因为前面只选择了前6名的较高权重，有可能把中心节点删除掉
    val neighborPairgroup = neighborPair1.union(neighborPair2).union(neighborPair3).union(neighborPair4).union(neighborPair5).union(neighborPair6).distinct
    val neighborPair = neighborPairgroup.join(fixEdgeWeightGraph.vertices).map(v => (v._1, (v._2._1, v._2._2)))

    //  .distinct//

    val returnNeighbor = neighborPair.aggregateByKey(Set[((VertexId, VertexId, Double, Double, Boolean), Double)]())(_ += _, _ ++ _).map { case (vid, vattr) =>
      var e = HashMap[(VertexId, VertexId), Double]()
      var v = HashMap[VertexId, Double]()

      vattr.foreach { case ((vSubId, vRelate, eWeight, vWeight, srcOrdst), vSubWeight) =>
        v.put(vSubId, vWeight)
        if (srcOrdst == true)
          e.put((vSubId, vRelate), eWeight)
        else
          e.put((vRelate, vSubId), eWeight)
      }
      if (!v.contains(vid)) {
        //  RDD里面不能套RDD，所以collect不到信息，会报空指针异常
        //  v.put(vid, fixEdgeWeightGraph.vertices.filter(_._1 == vid).map(_._2).head)
        v.put(vid, vattr.map(_._2).head)
      }
      /* val subVertex: List[MaxflowVertexAttr] = v.map(v => MaxflowVertexAttr(v._1, v._2)).toList
       val subEdge: List[MaxflowEdgeAttr] = e.map(e => MaxflowEdgeAttr(e._1._1, e._1._2, e._2)).toList
       (vid, subVertex, subEdge)*/

      var G = new MaxflowGraph()
      e.map { case ((src, dst), eweight) =>
        var a = v.filter(_._1 == src).toList.head
        var b = v.filter(_._1 == dst).toList.head
        G.addEdge(MaxflowVertexAttr(a._1, a._2), MaxflowVertexAttr(b._1, b._2), eweight)
      }
      (vid, G)
    }
    returnNeighbor
  }


  /**
    * 计算最大流分数
    */

  def run3(maxflowSubExtendPair: RDD[(VertexId, MaxflowGraph)], lambda: Double, threashold: Double, gainNum: Int) = {
    maxflowSubExtendPair.map { case (vid, vGraph) =>
      var vAndInflu: Set[(VertexId, Long, Double)] = Set() //(周边节点ID,周边节点初始纳税评分，单条传递分)
    var pairflow = 0D
      var allpairflow = 0D
      var fusionflow = 0D
      var returnflow = 0D
      //    val vid = 596267
      //   val vGraph=maxflowSubExtendPair.filter(_._1==vid).map(_._2).first()
      //    val src = vGraph.getGraph().keySet.filter(_.id == 936680).head
      //周边节点及其传递的最大流值
      var dst = vGraph.getGraph().keySet.filter(_.id == vid).head
      if (vGraph.getGraph().size > 1) {
        for (src <- vGraph.getGraph().keySet.-(dst).filter(_.initScore != 0)) {
          //调用最大流算法计算pair节点对内所有节点对其传递值
          val flowtemp = maxflowNotGraphX(vGraph, src, dst, threashold, gainNum)
          vAndInflu.add((src.id, (src.initScore * 100).toLong, flowtemp._3))
          //避免周边节点为100分时，公式中省略了这部分影响
          if (src.initScore == 1) {
            pairflow += flowtemp._3 // * (1 - src.initScore + 0.01)
            allpairflow += flowtemp._3 / src.initScore //* (1 - src.initScore + 0.01)
          } else {
            pairflow += flowtemp._3 // * (1 - src.initScore)
            allpairflow += flowtemp._3 / src.initScore //* (1 - src.initScore)
          }
        }
        //加入周边没有初始纳税信用评分的节点
        for (src <- vGraph.getGraph().keySet.-(dst).filter(_.initScore == 0)) {
          vAndInflu.add((src.id, 0, 0D))
        }
        //加入自身
        vAndInflu.add((vid, (dst.initScore * 100).toLong, 0D))
        if (allpairflow != 0) {
          fusionflow = lambda * dst.initScore + (1 - lambda) * pairflow / allpairflow
          returnflow = pairflow / allpairflow
        } else {
          fusionflow = dst.initScore
          returnflow = 0D
        }
        val vAndInfluAndRatio = vAndInflu.map { x =>
          var ratio = 0D
          if (allpairflow != 0) {
            if (x._2 == 100)
              ratio = x._3 * (1 - x._2 / 100D + 0.01) / allpairflow
            else
              ratio = x._3 * (1 - x._2 / 100D) / allpairflow
          }
          (x._1, x._2, x._3, ratio)
        }

        // (vid, returnflow, fusionflow, vAndInfluAndRatio.toList) //vAndInfluAndRatio(周边节点ID,周边节点初始纳税评分，单条传递分，单条传递分占总传递分的比值)
        //（中心节点ID，(1-β)后面，最终最大流得分，周边各节点流向中间的流量列表）
        (vid, returnflow, fusionflow, vAndInflu.toList)

      }
      else {
        //  (vid, 0D, lambda * dst.initScore, List[(VertexId, Long, Double, Double)]())
        (vid, 0D, lambda * dst.initScore, List[(VertexId, Long, Double)]())
      }
    }
  }

  def maxflowNotGraphX(vGraph0: MaxflowGraph, src: MaxflowVertexAttr, dst: MaxflowVertexAttr, threashold: Double, gainNum: Int = 1)
  : (MaxflowVertexAttr, MaxflowVertexAttr, Double) = {
    var fs = src.initScore
    //重新构图，注意引用问题，否则会导致在相同的vGraph上进行修改
    var vGraph = new MaxflowGraph
    vGraph0.getAllEdge().foreach { case e =>
      val s = new MaxflowVertexAttr(e.src.id, e.src.initScore)
      val d = new MaxflowVertexAttr(e.dst.id, e.dst.initScore)
      vGraph.addEdge(s, d, e.weight)
    }
    var maxflows = 0D
    var i = 1
    val empty = List[MaxflowVertexAttr]()
    while (fs > 0) {
      println(src.id)
      println("---->" + fs)
      val shortest = bfs4(src, dst, vGraph, fs, threashold, gainNum)
      println(shortest)
      println()
      if (shortest != empty) {
        var path = Set[MaxflowEdgeAttr]()
        for (i <- 0 until shortest.size - 1) {
          path = path.+(MaxflowEdgeAttr(shortest(i), shortest(i + 1), shortest(i + 1).capacity))
        }
        val edgeTemp = vGraph.getAllEdge()

        for (a <- path) {
          for (b <- edgeTemp) {
            if (a == b) {
              b.weight = b.weight - a.weight //修正正向弧
              vGraph.addEdge(a.dst, a.src, a.weight) //添加反向弧
            }
          }
        }
        fs -= shortest(1).capacity
      } else
        return (src, dst, maxflows)
      maxflows += shortest.last.capacity
    }
    return (src, dst, maxflows)
  }


  //第四种最大流方法【最终正确版本】
  def bfs4(src: MaxflowVertexAttr, dst: MaxflowVertexAttr, vGraph: MaxflowGraph, fs: Double, threashold: Double, gainNum: Int): List[MaxflowVertexAttr] = {
    //  重新构图
    var residual = new MaxflowGraph
    vGraph.getAllEdge().foreach { case e =>
      val s = new MaxflowVertexAttr(e.src.id, e.src.initScore)
      val d = new MaxflowVertexAttr(e.dst.id, e.dst.initScore)
      residual.addEdge(s, d, e.weight)
    }

    var queue = List[MaxflowVertexAttr]()
    //初始节点更新距离为0，容量为fs.
    residual.getGraph().keySet.find(_ == src).get.update(0, fs)
    queue = residual.getGraph().keySet.find(_ == src).get +: queue

    while (!queue.isEmpty) {
      //更新图
      for (a <- queue) {
        residual.getGraph().keySet.filter(x => (x == a)).head.update(a.distance, a.capacity)
        val edgetemp = residual.getAllEdge()
        for (b <- edgetemp) {
          if (b.src == a)
            b.src.update(a.distance, a.capacity)
          if (b.dst == a)
            b.dst.update(a.distance, a.capacity)
        }
      }

      queue = queue.sortWith { (p1: MaxflowVertexAttr, p2: MaxflowVertexAttr) =>
        p1.distance == p2.distance match {
          case false => -p1.distance.compareTo(p2.distance) > 0
          case _ => p1.capacity - p2.capacity > 0
        }
      }

      var top = queue.head
      queue = queue.drop(1)

      for (edge <- residual.getAdj(top)) {
        //top的邻居结点，（仅以top为源节点的）
        if (edge.src.distance + 1 < edge.dst.distance && edge.weight > threashold) { //??标记更新
          val candi = residual.getGraph().keySet.find(_ == edge.dst).get
          candi.distance = edge.src.distance + 1
          if (candi == dst) {
            candi.capacity = Math.min(Math.min(edge.src.capacity * gain(edge.src.distance, gainNum), edge.weight), fs)
          } else {
            candi.capacity = Math.min(Math.min(Math.min(edge.src.capacity, edge.weight) * gain(edge.src.distance, gainNum), edge.weight), fs)
          }
          candi.edgeTo = top.id
          queue = candi +: queue
        }
      }
    }

    // 检查是否有路径
    if (residual.getGraph().keySet.find(_ == dst).get.capacity != (Double.MaxValue - 1)) {
      var links = new LinkedHashMap[MaxflowVertexAttr, MaxflowVertexAttr]
      links += residual.getGraph().keySet.find(_ == dst).get -> null
      while (links.keySet.last.id != src.id) {
        links += residual.getGraph().keySet.find(_.id == residual.getGraph().keySet.find(_ == links.keySet.last).get.edgeTo).get -> links.keySet.last
      }
      parse(links, residual.getGraph().keySet.find(_.id == src.id).get)
    }
    else
      return List()

  }

  //解析路径
  def parse(path: LinkedHashMap[MaxflowVertexAttr, MaxflowVertexAttr], key: MaxflowVertexAttr): List[MaxflowVertexAttr] = {
    key match {
      case null => Nil
      case _ => val value = path.get(key).get
        key :: parse(path -= key, value)
    }
  }


  /**
    * 每个节点的泄露函数
    **/
  def gain(x: Int, gainNum: Int = 1): Double = {
    var toReturn: Double = 0D
    if (x == 0)
      toReturn = 1
    else {
      gainNum match {
        case 1 => toReturn = 0.8
        case 2 => toReturn = math.cos(0.09 * x)
        case 3 => toReturn = 1 - math.pow(Math.E, -x)
        case 4 => toReturn = 1 - math.pow(x + 1, -4)
      }
    }
    toReturn
  }


}


