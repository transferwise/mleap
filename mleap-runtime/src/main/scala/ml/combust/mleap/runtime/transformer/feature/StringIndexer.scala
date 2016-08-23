package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.feature.StringIndexerModel
import ml.combust.mleap.runtime.attribute.{AttributeSchema, CategoricalAttribute}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder.Ops
import ml.combust.mleap.runtime.types.DoubleType

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexer(uid: String = Transformer.uniqueName("string_indexer"),
                         inputCol: String,
                         outputCol: String,
                         model: StringIndexerModel) extends Transformer {
  override def build[TB: TransformBuilder](builder: TB): Try[TB] = {
    builder.withInput(inputCol).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, DoubleType)(row => model(row.get(inputIndex).toString))
    }
  }

  override def transformAttributeSchema(schema: AttributeSchema): AttributeSchema = {
    schema.withField(outputCol, CategoricalAttribute())
  }

  def toReverse: ReverseStringIndexer = ReverseStringIndexer(inputCol = inputCol,
    outputCol = outputCol,
    indexer = model.toReverse)

  def toReverse(name: String): ReverseStringIndexer = ReverseStringIndexer(uid = name,
    inputCol = inputCol,
    outputCol = outputCol,
    indexer = model.toReverse)
}
