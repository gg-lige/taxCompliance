package com.lg.consturct

import java.math.BigDecimal

import com.lg.entity.impl.{E_TESNAttr, V_TESNAttr}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import utils.Parameters

/**
  * Created by lg on 2018/11/15.
  */
object TESNTools {
  val logger: Logger = LoggerFactory.getLogger(this.getClass())
  val hdfsDir: String = Parameters.Dir
  val db = Map("url" -> Parameters.DataBaseURL,
    "driver" -> Parameters.JDBCDriverString,
    "user" -> Parameters.DataBaseUserName,
    "password" -> Parameters.DataBaseUserPassword)

  /**
    * 从数据库中读取构建初始图的点和边,重新编完号后投资、法人、股东边存入数据库添加反向影响，点存入HDFS
    */
  def saveE2Oracle_V2HDFS(sqlContext: SparkSession) = {

    import sqlContext.implicits._
    val FR_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_NSR_FDDBR"))).load()
    val TZ_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_NSR_TZF"))).load()
    val GD_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_NSR_GD"))).load()
    //  val JY_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_XFNSR_GFNSR"))).load()
    //    val XYJB_DF = sqlContext.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_GROUNDTRUTH"))).load()
    //   val xyjb = XYJB_DF.select("VERTEXID", "XYGL_XYJB_DM", "FZ", "WTBZ").rdd
    //     .map(row => (row.getAs[BigDecimal]("VERTEXID").longValue(), (row.getAs[BigDecimal]("FZ").intValue(), row.getAs[String]("XYGL_XYJB_DM"), row.getAs[String]("WTBZ"))))


    //计算点表(先计算出所有纳税人节点，在计算所有非纳税人节点)
    //抽出投资方为纳税人的数据行
    val TZ_NSR_DF = TZ_DF.filter($"TZFXZ".startsWith("1") || $"TZFXZ".startsWith("2") || $"TZFXZ".startsWith("3"))
      .selectExpr("ZJHM as TZ_ZJHM", "VERTEXID as BTZ_VERTEXID", "TZBL", "TZFMC AS NAME")
    //抽出股东为纳税人的数据行
    val GD_NSR_DF = GD_DF.filter($"JJXZ".startsWith("1") || $"JJXZ".startsWith("2") || $"JJXZ".startsWith("3"))
      .selectExpr("ZJHM as TZ_ZJHM", "VERTEXID as BTZ_VERTEXID", "TZBL", "GDMC AS NAME")
    val ZJHM_NSR_DF = TZ_NSR_DF.unionAll(GD_NSR_DF)
    val NSR_VERTEX = ZJHM_NSR_DF.selectExpr("TZ_ZJHM AS ZJHM").except(FR_DF.selectExpr("ZJHM")) //投资方与股东表中投资方的证件号码除去法人表中的法人的证件号码
      .join(ZJHM_NSR_DF, $"ZJHM" === $"TZ_ZJHM").select("TZ_ZJHM", "NAME") //再join原dataframe是为了得到名称
      .rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("TZ_ZJHM"), true))

    //抽出投资方为非纳税人的数据行
    val TZ_FNSR_DF = TZ_DF.filter($"TZFXZ".startsWith("4") || $"TZFXZ".startsWith("5"))
    //抽出股东为非纳税人的数据行
    val GD_FNSR_DF = GD_DF.filter($"JJXZ".startsWith("4") || $"JJXZ".startsWith("5"))
    val FNSR_VERTEX = FR_DF.selectExpr("ZJHM", "FDDBRMC AS NAME")
      .unionAll(TZ_FNSR_DF.selectExpr("ZJHM", "TZFMC AS NAME"))
      .unionAll(GD_FNSR_DF.selectExpr("ZJHM", "GDMC AS NAME"))
      .rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("ZJHM"), false))

    val maxNsrID = FR_DF.agg(max("VERTEXID")).head().getDecimal(0).longValue()
    val NSR_FNSR_VERTEX = FNSR_VERTEX.union(NSR_VERTEX).map { case (name, sbh, isNSR) => (sbh, V_TESNAttr(name, sbh, isNSR)) }
      .reduceByKey(V_TESNAttr.combine).zipWithIndex().map { case ((nsrsbh, attr), index) => (index + maxNsrID, attr) }

    val ALL_VERTEX = NSR_FNSR_VERTEX.union(FR_DF.select("VERTEXID", "NSRDZDAH", "NSRMC").rdd.map(row =>
      (row.getAs[BigDecimal]("VERTEXID").longValue(), V_TESNAttr(row.getAs[String]("NSRMC"), row.getAs[BigDecimal]("NSRDZDAH").toString, true))))
      /* .leftOuterJoin(xyjb)
       .map { case (vid, (vattr, opt_fz_dm)) =>
         if (!opt_fz_dm.isEmpty) {
           vattr.xyfz = opt_fz_dm.get._1
           vattr.xydj = opt_fz_dm.get._2
           if (opt_fz_dm.get._3.equals("Y"))
             vattr.wtbz = 1
         }
         (vid, vattr)
       }*/ .persist(StorageLevel.MEMORY_AND_DISK)

    //计算边表
    //投资方为纳税人（表示为投资方证件号码对应法人表证件号码所对应的公司）的投资关系
    val tz_cc = TZ_NSR_DF.
      join(FR_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = E_TESNAttr(0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0, 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }

    val gd_cc = GD_NSR_DF.
      join(FR_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = E_TESNAttr(0.0, 0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }

    //投资方为非纳税人的投资关系
    val tz_pc_cc = TZ_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(TZ_NSR_DF.join(FR_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(NSR_FNSR_VERTEX.keyBy(_._2.sbh)).
      map { case (sbh1, ((dstid, tzbl), (srcid, attr))) =>
        val eattr = E_TESNAttr(0.0, tzbl, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }

    val gd_pc_cc = GD_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(GD_NSR_DF.join(FR_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(NSR_FNSR_VERTEX.keyBy(_._2.sbh)).
      map { case (sbh1, ((dstid, gdbl), (srcid, attr))) =>
        val eattr = E_TESNAttr(0.0, 0.0, gdbl, 0.0)
        ((srcid, dstid), eattr)
      }

    val fddb_pc = FR_DF.select("VERTEXID", "ZJHM").
      rdd.map(row => (row.getAs[String](1), row.getAs[BigDecimal](0).longValue())).
      join(NSR_FNSR_VERTEX.keyBy(_._2.sbh)).
      map { case (sbh1, (dstid, (srcid, attr))) =>
        val eattr = E_TESNAttr(1.0, 0.0, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }
    // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
    val ALL_EDGE = tz_cc.union(gd_cc).union(tz_pc_cc).union(gd_pc_cc).union(fddb_pc).
      reduceByKey(E_TESNAttr.combine).filter(edge => edge._1._1 != edge._1._2).
      map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
      persist(StorageLevel.MEMORY_AND_DISK)

    //将所有点存入hdfs
    ALL_VERTEX.saveAsObjectFile(s"${hdfsDir}/lg_startVertices")

    // JdbcUtils.execute("truncate table " + "lg_startEdge") //注意词表不是很准确，因为集群崩时重跑，hdfs 上的是准确的
    val schema = StructType(
      List(
        StructField("SRC", LongType, true),
        StructField("DST", LongType, true),
        StructField("W_LEGAL", DoubleType, true),
        StructField("W_INVEST", DoubleType, true),
        StructField("W_STOCKHOLDER", DoubleType, true)
      )
    )

    val rowRDD = ALL_EDGE.filter(e => e.attr.w_invest != 0.0 || e.attr.w_legal != 0.0 || e.attr.w_stockholder != 0.0).map(p => Row(p.srcId, p.dstId, p.attr.w_legal, p.attr.w_invest, p.attr.w_stockholder)).distinct()
    val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    val options = new JDBCOptions(db + ("dbtable" -> "lg_startEdge"))
    JdbcUtils.saveTable(edgeDataFrame, Option(schema), false, options)
  }


  def constructTesnMethod(sparkSession: SparkSession): Graph[V_TESNAttr, E_TESNAttr] = {
    val FR_DF = sparkSession.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_FR2"))).load()
    val TZ_DF = sparkSession.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_TZ2"))).load()
    val GD_DF = sparkSession.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_GD2"))).load()
    val JY_DF = sparkSession.read.format("jdbc").options(db + (("dbtable" -> "tax.LG_XFNSR_GFNSR"))).load()

    //计算边表
    //法人边正反影响融合
    val fr = FR_DF.select("SRC", "DST", "W_LEGAL").
      rdd.map(row => ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), row.getAs[BigDecimal](2).doubleValue())).
      reduceByKey((a, b) => {
        val f_positive = a * b //正向融合因子
        val f_inverse = (1 - a) * (1 - b)
        f_positive / (f_positive + f_inverse)
      }).map { case row =>
      val eattr = E_TESNAttr(row._2, 0.0, 0.0, 0.0)
      (row._1, eattr)
    }

    //投资边正反影响融合
    val tz = TZ_DF.select("SRC", "DST", "W_INVEST").
      rdd.map(row => ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), row.getAs[BigDecimal](2).doubleValue())).
      reduceByKey((a, b) => {
        val f_positive = a * b
        val f_inverse = (1 - a) * (1 - b)
        f_positive / (f_positive + f_inverse)
      }).map { case row =>
      val eattr = E_TESNAttr(0.0, row._2, 0.0, 0.0)
      (row._1, eattr)
    }

    //股东边正反影响融合
    val gd = GD_DF.select("SRC", "DST", "W_STOCKHOLDER").
      rdd.map(row => ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), row.getAs[BigDecimal](2).doubleValue())).
      reduceByKey((a, b) => {
        val f_positive = a * b
        val f_inverse = (1 - a) * (1 - b)
        f_positive / (f_positive + f_inverse)
      }).map { case row =>
      val eattr = E_TESNAttr(0.0, 0.0, row._2, 0.0)
      (row._1, eattr)
    }


    //========================================
    /*
    val tz_forward = tz_cc.union(tz_pc_cc).reduceByKey(InitEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2)
    val tz_backward = tz_forward.collect.map {
      case (v, e) =>
        val srcOutSum = tz_forward.filter(_._1._1 == v._1).map(_._2.w_invest).reduce(_ + _)
        ((v._2, v._1), InitEdgeAttr(0.0, e.w_invest / srcOutSum, 0.0, 0.0))
    }
    val tz_fusion= tz_forward.union(tz_backward).reduceByKey((a,b)=> {
      val f_invest_positive = a.w_invest * b.w_invest
      val f_invest_inverse = (1 - a.w_invest) * (1 - b.w_invest)
      InitEdgeAttr(a.w_legal,f_invest_positive / (f_invest_positive + f_invest_inverse),a.w_stockholder,a.w_trade)
    })
  */
    //========================================

    val jy = JY_DF.
      select("xf_VERTEXID", "gf_VERTEXID", "jybl", "je", "se", "sl").
      rdd.map { case row =>
      val eattr = E_TESNAttr(0.0, 0.0, 0.0, row.getAs[BigDecimal]("jybl").doubleValue())
      eattr.trade_je = row.getAs[BigDecimal]("je").doubleValue()
      eattr.tax_rate = row.getAs[BigDecimal]("sl").doubleValue()
      ((row.getAs[BigDecimal]("xf_VERTEXID").longValue(), row.getAs[BigDecimal]("gf_VERTEXID").longValue()), eattr)
    }

    // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环,交易边选择此为避免出现<0的边，可能存在 交易边不是双向边
    val ALL_EDGE = fr.union(tz).union(gd).union(jy.filter(x => x._2.w_trade > 0.01)).
      reduceByKey(E_TESNAttr.combine).filter(edge => edge._1._1 != edge._1._2).
      map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
      persist(StorageLevel.MEMORY_AND_DISK)

    val ALL_VERTEX = sparkSession.sparkContext.objectFile[(Long, V_TESNAttr)](s"${hdfsDir}/lg_startVertices").repartition(128)
    val degrees = Graph(ALL_VERTEX, ALL_EDGE).degrees.persist
    // 使用度大于0的顶点和边构建图
    Graph(ALL_VERTEX.join(degrees).map(vertex => (vertex._1, vertex._2._1)), ALL_EDGE) /*.subgraph(vpred = (vid, vattr) => vattr.xyfz >= 0)*/ .persist()

  }

}
