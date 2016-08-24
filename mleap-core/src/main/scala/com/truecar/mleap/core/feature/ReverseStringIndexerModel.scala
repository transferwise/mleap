package com.truecar.mleap.core.feature

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class ReverseStringIndexerModel(labels: Seq[String]) {
  val indexToString: Map[Int, String] = labels.zipWithIndex.map(v => (v._2, v._1)).toMap

  def apply(index: Int): String = indexToString(index)
}
