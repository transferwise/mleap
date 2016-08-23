package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.feature.ReverseStringIndexerModel
import ml.combust.mleap.runtime.attribute.{AttributeSchema, CategoricalAttribute}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder.Ops
import ml.combust.mleap.runtime.types.StringType

import scala.util.Try

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class ReverseStringIndexer(uid: String = Transformer.uniqueName("reverse_string_indexer"),
                                inputCol: String,
                                outputCol: String,
                                indexer: ReverseStringIndexerModel) extends Transformer {
  override def build[TB: TransformBuilder](builder: TB): Try[TB] = {
    builder.withInput(inputCol).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, StringType)(row => indexer(row.getDouble(inputIndex).toInt))
    }
  }

  override def transformAttributeSchema(schema: AttributeSchema): AttributeSchema = {
    schema.withField(outputCol, CategoricalAttribute())
  }
}
