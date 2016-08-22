package com.truecar.mleap.core.feature

/**
  * Created by hwilkins on 12/30/15.
  */
object TokenizerModel {
  val defaultTokenizer = TokenizerModel("\\s")
}

case class TokenizerModel(regex: String = "\\s") {
  def apply(document: String): Array[String] = document.toLowerCase.split(regex)
}
