package com.lg.entity.impl

/**
  * Created by lg on 2017/6/22.
  */
class V_TGNAttr(var sbh: String, var name: String) extends Serializable {

  var compliance: Double = 0d
  var wtbz: Int = -1

  override def toString = s" $sbh, $name，$compliance,$wtbz"
}

object V_TGNAttr {
  def apply(sbh: String, name: String): V_TGNAttr = new V_TGNAttr(sbh, name)
}
