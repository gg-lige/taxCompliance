package com.lg.consturct

import com.lg.entity.impl.{E_TESNAttr, E_TGNAttr, V_TESNAttr, V_TGNAttr}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.slf4j.{Logger, LoggerFactory}
import utils.Parameters

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by lg on 2018/11/15.
  */
object TGNTools {
  val logger: Logger = LoggerFactory.getLogger(this.getClass())
  val hdfsDir: String = Parameters.Dir
  val db = Map("url" -> Parameters.DataBaseURL,
    "driver" -> Parameters.JDBCDriverString,
    "user" -> Parameters.DataBaseUserName,
    "password" -> Parameters.DataBaseUserPassword)
  type Path = Seq[(VertexId, Double)]
  type Paths = Seq[Seq[(VertexId, Double)]]

  /**
    * 添加企业之间的亲密度关系
    */
  def addCohesion(tpinFromObject: Graph[V_TESNAttr, E_TESNAttr], weight: Double) = {
    //每个非纳税人直接或间接控制的企业列表
    val initialGraph = tpinFromObject.mapVertices { case (id, vattr) =>
      if (!vattr.isNSR) //非纳税人
        Seq(Seq((id, 1D))) //10D表示1的Double类型
      else //纳税人
        Seq[Seq[(VertexId, Double)]]()
    }.subgraph(epred = triplet =>
      triplet.attr.isAntecedent(weight)).mapEdges(edge => Seq(edge.attr.w_legal, edge.attr.w_invest, edge.attr.w_stockholder).max).cache() //1.路径为1的边有多种关系时，取其中权重最大的

    //信息（公司id,Map(自然人id,权重)）：此处无法使用反向获取路径，,即使用正向获取路径，要求源点为人 ，maxIteration表示前件路径最长长度为3
    val messageOfControls = getPath(remove0Degree(initialGraph), sendPaths, reducePaths, maxIteration = 3)
    val reverseMessageOfControls = getPath(remove0Degree(initialGraph), sendPathsReverse, reducePaths, maxIteration = 3)

    val allmessageOfcontrols = messageOfControls.flatMap(_._2).groupBy(_.head._1).union(reverseMessageOfControls.flatMap(_._2).groupBy(_.last._1)).reduceByKey(_ ++ _).
      mapValues {
        val result = mutable.HashMap[(Long, Long), (Double, Int)]()
        lists =>
          lists.filter(_.size > 1).foreach { case perlist =>
            val influ = perlist.map(_._2).min //2. 单条链取最小
          val length = perlist.size

            if (!result.contains((perlist.head._1, perlist.last._1)))
              result.put((perlist.head._1, perlist.last._1), (influ, length))
            else if (result.contains((perlist.head._1, perlist.last._1)) && result.get((perlist.head._1, perlist.last._1)).get._2 > length) //3.同源终时， 取最短路径
              result.update((perlist.head._1, perlist.last._1), (influ, length))
          }
          result.map(x => (x._1, x._2._1)).toSeq
      }.filter(_._2.size > 0).join(tpinFromObject.vertices.filter(_._2.isNSR == false)).map(x => (x._1, x._2._1)) //保证链均在自然人上


    def computeCohesionWeight(vid: VertexId, d: Seq[((Long, Long), Double)]) = {
      var overlap = d
      //构造由 reverselist 到 forwardlist 的链表组合
      val reverselist = overlap.groupBy(_._1._2).filter(_._1 == vid).flatMap(_._2.map(x => (x._1._1, x._2))) //按尾聚合
      val forwardlist = overlap.groupBy(_._1._1).filter(_._1 == vid).flatMap(_._2.map(x => (x._1._2, x._2)))  //按头聚合
      val result =
        for (head <- reverselist) yield
          for (last <- forwardlist) yield {
            if (head._1 != last._1) {
              Option(((head._1, last._1), head._2.min(last._2))) // 4.取最小
            } else
              Option.empty
          }
      result.flatten.filter(!_.isEmpty).map(_.get)
    }

    val newCohesionE = allmessageOfcontrols.map { case (vid, lists) => computeCohesionWeight(vid, lists) }.flatMap(_.toList).reduceByKey(_ + _).subtractByKey(initialGraph.edges.map(e => ((e.srcId, e.dstId), 1)))

    //1.等比归一化
    val cohe_max = newCohesionE.map(_._2).max
    val cohe_min = newCohesionE.map(_._2).min

    //2.超过百分比归一化
    val allSum = newCohesionE.count.toDouble
    val weightTemp = newCohesionE.map(x => (x._2, 1)).reduceByKey(_ + _).sortByKey().collect
    val weightMap = weightTemp.map(e => (e._1, weightTemp.filter(_._1 < e._1).map(_._2).sum / allSum)).toMap


    val newCohesionEdges = newCohesionE.map { case ((src, dst), weight) =>
      val edgeAttr = E_TESNAttr()
      edgeAttr.is_Cohesion = true
      // edgeAttr.w_cohesion = (weight/(cohe_max-cohe_min)).formatted("%.3f").toDouble
      edgeAttr.w_cohesion = weightMap.getOrElse(weight, 0D)

      Edge(src, dst, edgeAttr)
    }

    val newEdge = tpinFromObject.edges.union(newCohesionEdges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(E_TESNAttr.combine).filter(edge => edge._1._1 != edge._1._2)
      .map(e => Edge(e._1._1, e._1._2, e._2))
    Graph(tpinFromObject.vertices, newEdge)
  }


  /**
    * 得到路径
    * direction=1 表示从源向终传递消息 ； -1从终向源
    *
    */
  def getPath[VD: ClassTag, ED: ClassTag](initialGraph: Graph[VD, ED],
                                          sendPaths: (EdgeContext[VD, ED, VD], Int) => Unit,
                                          reducePaths: (VD, VD) => VD,
                                          maxIteration: Int = Int.MaxValue,
                                          direction: Int = 1,
                                          initLength: Int = 1) = {

    //使用度大于0的顶点和边构建前件网络图（得到点前件路径【空】，边重新计算权值）
    var preprocessGraph = initialGraph.cache()

    //路径长度（当迭代路径为2时，可能出现人-公司-人的情况）
    var i = initLength;
    var messages: VertexRDD[VD] = null
    //message的长度为i+1
    if (direction == 1)
      messages = preprocessGraph.aggregateMessages[VD](sendPaths(_, i), reducePaths)
    //    else
    //      messages = preprocessGraph.aggregateMessages[VD](sendPathsReverse(_, i), reducePaths)

    var activeMessages = messages.count()
    var preG: Graph[VD, ED] = null
    while (activeMessages > 0 && i <= maxIteration) {
      preG = preprocessGraph
      //长度=maxIteration+1
      preprocessGraph = preprocessGraph.joinVertices[VD](messages)((id, oldVD, newPath) => reducePaths(oldVD, newPath)).cache() //迭代给点上记录路径信息
      println(i + " 次迭代完成！")
      i += 1

      val oldMessages = messages
      if (direction == 1)
        messages = preprocessGraph.aggregateMessages[VD](sendPaths(_, i), reducePaths).cache()
      //      else
      //        messages = preprocessGraph.aggregateMessages[VD](sendPathsReverse(_, i), reducePaths).cache()
      activeMessages = messages.count()
      oldMessages.unpersist(blocking = false)
      preG.unpersistVertices(blocking = false)
      preG.edges.unpersist(blocking = false)
    }
    preprocessGraph.vertices
  }


  def remove0Degree[VD: ClassTag, ED: ClassTag](tpin: Graph[VD, ED]): Graph[VD, ED] = {
    val degreesRDD = tpin.degrees.cache()
    var preproccessedGraph = tpin.
      outerJoinVertices(degreesRDD)((vid, vattr, degreesVar) => (vattr, degreesVar.getOrElse(0))).
      subgraph(vpred = {
        case (vid, (vattr, degreesVar)) =>
          degreesVar > 0
      }).
      mapVertices {
        case (vid, (attr, degree)) => attr
      }
    preproccessedGraph
  }


  def sendPaths(edge: EdgeContext[Paths, Double, Paths], length: Int) = { //EdgeContext传递的为[VD, ED, A]
    //得到非纳税人控制的关系链，所以以非纳税人为起点，初始长度中非纳税人为1，纳税人为0。过滤掉非起点 (得到与所需length一样的路径,因为起点必须是非纳税人) 与 非环
    val filterEdge = edge.srcAttr.filter(_.size == length).filter(!_.map(_._1).contains(edge.dstId)) //过滤源点属性（长度为length，非环）
  val filterEdge2 = edge.dstAttr.filter(_.size == 1) //过滤终点属性（长度为1）

    if ((length != 1 && filterEdge.size > 0 && filterEdge2.size == 0) ||
      (length == 1 && filterEdge.size > 0))
    //向终点发送控制人关系路径集合
      edge.sendToDst(filterEdge.map(_ ++ Seq((edge.dstId, edge.attr))))
  }

  def reducePaths(a: Paths, b: Paths): Paths = a ++ b

  def sendPathsReverse(edge: EdgeContext[Paths, Double, Paths], length: Int) = {
    val filterEdge = edge.dstAttr.filter(_.size == length).filter(!_.map(_._1).contains(edge.srcId))
    val filterEdge2 = edge.srcAttr.filter(_.size == 1) //过滤终点属性（长度为1）
    if ((length != 1 && filterEdge.size > 0 && filterEdge2.size == 0) ||
      (length == 1 && filterEdge.size > 0))
      edge.sendToSrc(filterEdge.map(Seq((edge.srcId, edge.attr)) ++ _))
  }


  /**
    * 抽取出仅含公司的tpin大图
    */
  def extractNSR(graph: Graph[V_TESNAttr, E_TESNAttr]): Graph[V_TGNAttr, E_TGNAttr] = {
    //   val cohesionDifference = graph.edges.map(x => (x.attr.w_cohesion)).max - graph.edges.map(x => (x.attr.w_cohesion)).min
    val g = graph.subgraph(vpred = (vid, vattr) => vattr.isNSR == true).
      mapVertices { (vid, vattr) =>
        val newVattr = V_TGNAttr(vattr.sbh, vattr.name)
        /*
                newVattr.xydj = vattr.xydj
                newVattr.xyfz = vattr.xyfz
                newVattr.wtbz = vattr.wtbz
        */
        newVattr
      }.
      mapEdges { edge =>
        val newEattr = E_TGNAttr()
        if (edge.attr.w_invest > 1D)
          newEattr.w_invest = 1.0
        else
          newEattr.w_invest = edge.attr.w_invest
        if (edge.attr.w_stockholder > 1D)
          newEattr.w_stockholder = 1.0
        else
          newEattr.w_stockholder = edge.attr.w_stockholder
        if (edge.attr.w_trade > 1D)
          newEattr.w_trade = 1.0
        else
          newEattr.w_trade = edge.attr.w_trade
        newEattr.w_cohesion = edge.attr.w_cohesion //  /cohesionDifference
        newEattr
      }

    //过滤掉边上无权值的
    val filter_E = g.edges.filter(edge => edge.attr.w_cohesion != 0.0 || edge.attr.w_invest != 0.0 || edge.attr.w_stockholder != 0.0 || edge.attr.w_trade != 0.0)
    val filterGraph = Graph(g.vertices, filter_E)
    val vertexDegree = filterGraph.degrees.persist()
    Graph(filterGraph.vertices.join(vertexDegree).map(v => (v._1, v._2._1)), filterGraph.edges)

  }


  def saveVertex(session: SparkSession, result: RDD[(VertexId,Long)], dst: String = "LG_TGNVertex") = {
    import session.implicits._
    val rowRDD = result.toDF("VERTEXID","NSRDZDAH")
    val jOptions = new JDBCOptions(db + (("dbtable", dst)))
    val conn = JdbcUtils.createConnectionFactory(jOptions)()
    if (JdbcUtils.tableExists(conn, jOptions)) {
      JdbcUtils.truncateTable(conn, dst)
    } else {
      JdbcUtils.createTable(conn, rowRDD, jOptions)
    }
    JdbcUtils.saveTable(rowRDD, Option(rowRDD.schema), false, jOptions)
    conn.close()
  }

}
