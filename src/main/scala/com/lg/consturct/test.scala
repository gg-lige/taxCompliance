package com.lg.consturct

import com.lg.entity.impl.{E_TGNAttr, V_TGNAttr}
import com.lg.utils.HdfsTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{SQLContext, SparkSession}
import utils.Parameters
import org.apache.spark.sql.SparkSession

/**
  * Created by lg on 2018/12/5.
  *
  *
  *   ./spark-shell --master spark://a00581a874d9:7077 --executor-memory 4G --total-executor-cores 2 --driver-memory 2G
  */

object test {
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
    sc.setLogLevel("OFF")
    val sqlContext = sparkSession.sqlContext

    val s2=SparkSession.builder().enableHiveSupport().getOrCreate()
    s2.sql("select id,name from hive.music where dt='20190606'").write.format("parquet").save("/opt/music.parquet")


  }
}
