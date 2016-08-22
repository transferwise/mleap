package com.truecar.mleap.core.feature

/**
 * Created by hwilkins on 11/5/15.
 */
case class StringIndexerModel(labels: Seq[String]) extends Serializable {
  val stringToIndex: Map[String, Int] = labels.zipWithIndex.toMap

  def apply(value: String): Double = stringToIndex(value)

  def toReverse: ReverseStringIndexerModel = ReverseStringIndexerModel(labels)
}
