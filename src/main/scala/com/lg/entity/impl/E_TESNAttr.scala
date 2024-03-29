package com.lg.entity.impl

import com.lg.entity.E_Attr

/**
  * Created by lg on 2017/6/20.
  */
//类上肯定具有的四列属性，法人、投资、股东、交易
class E_TESNAttr(var w_legal: Double, var w_invest: Double, var w_stockholder: Double, var w_trade: Double) extends E_Attr {

  //税率、交易金额、是否由亲密关系、亲密关系权重
  var tax_rate: Double = 0.0
  var trade_je: Double = 0.0
  var is_Cohesion: Boolean = false
  var w_cohesion: Double = 0.0


  override def toString = s"E_TESNAttr(法人:$w_legal, 投资:$w_invest, 股东:$w_stockholder, 交易:$w_trade)"


  //前件路径（除了交易的其他关系组成的路径），无论正反向路径
  def isAntecedent(weight:Double): Boolean = {
    if (this.is_Cohesion)
      return false
    (this.w_legal > weight || this.w_invest > weight || this.w_stockholder > weight)

  }

  def isTrade(): Boolean = {
    this.w_trade != 0.0
  }

}

object E_TESNAttr {
  /*
  def fusion(a: InitEdgeAttr, b: InitEdgeAttr) = {
    val f_legal_positive = a.w_legal * b.w_legal
    val f_legal_inverse = (1 - a.w_legal) * (1 - b.w_legal)
    val f_invest_positive = a.w_invest * b.w_invest
    val f_invest_inverse = (1 - a.w_invest) * (1 - b.w_invest)
    val f_stockholder_positive = a.w_stockholder * b.w_stockholder
    val f_stockholder_inverse = (1 - a.w_stockholder) * (1 - b.w_stockholder)
    val toReturn = new InitEdgeAttr(f_legal_positive / (f_legal_positive + f_legal_inverse), f_invest_positive / (f_invest_positive + f_invest_inverse), f_stockholder_positive / (f_stockholder_positive + f_stockholder_inverse), 0.0)
    toReturn
  }
*/
  def apply(w_legal: Double = 0.0, w_invest: Double = 0.0, w_stockholder: Double = 0.0, w_trade: Double = 0.0): E_TESNAttr = {
    val re_legal = if (w_legal > 1.0) 1.0 else w_legal
    val re_invest = if (w_invest > 1.0) 1.0 else w_invest
    val re_stockholder = if (w_stockholder > 1.0) 1.0 else w_stockholder
    val re_trade = if (w_trade > 1.0) 1.0 else w_trade
    new E_TESNAttr(re_legal, re_invest, re_stockholder, re_trade)
  }

  def combine(a: E_TESNAttr, b: E_TESNAttr) = {
    val toReturn = new E_TESNAttr(a.w_legal + b.w_legal, a.w_invest + b.w_invest, a.w_stockholder + b.w_stockholder, a.w_trade + b.w_trade)
    toReturn.trade_je = a.trade_je + b.trade_je
    toReturn.w_cohesion = a.w_cohesion + b.w_cohesion
    toReturn.is_Cohesion = a.is_Cohesion || b.is_Cohesion
    if (toReturn.w_trade > 1D) toReturn.w_trade = 1.0 //组合时注意税率不同的交易边的组合，这里采用的是求和,若求和大于1.0 置为1
    toReturn
  }
}


