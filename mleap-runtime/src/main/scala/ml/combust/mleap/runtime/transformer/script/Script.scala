package ml.combust.mleap.runtime.transformer.script

import ml.combust.mleap.runtime.function.FieldSelector
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by hollinwilkins on 12/26/16.
  */
case class Script(override val uid: String = Transformer.uniqueName("script"),
                  inputCols: Seq[String],
                  outputCol: String,
                  model: ScriptModel) extends Transformer {
  val exec = model.udf
  val selectors = inputCols.map(FieldSelector)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, selectors: _*)(exec)
  }
}
