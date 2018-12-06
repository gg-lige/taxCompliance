package com.lg.design

import com.lg.design.impl.{MoleTrustMode, PeerTrustMode, maxflowCreditMode, pageRankMode}
import utils.Parameters

import scala.collection.Seq

/**
  * Created by lg on 2018/12/5.
  */
object ModeExperiment {

  //对比实验
  def main(args: Array[String]): Unit = {
    modeExperiment()
  }

  def modeExperiment() = {
    val hdfsDir: String = Parameters.Dir
    val methods = Seq(new maxflowCreditMode(), new pageRankMode(), new PeerTrustMode(),new MoleTrustMode())
    methods.map(method => {
      println(s"\r开始对比实验=> ${method.description}")
      method.run()
    })
    println("\n结束")
  }
}
