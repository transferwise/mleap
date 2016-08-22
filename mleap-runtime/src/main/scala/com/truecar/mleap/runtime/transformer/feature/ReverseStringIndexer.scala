package com.truecar.mleap.runtime.transformer.feature

import com.truecar.mleap.core.feature.ReverseStringIndexerModel
import com.truecar.mleap.runtime.attribute.{AttributeSchema, CategoricalAttribute}
import com.truecar.mleap.runtime.transformer.Transformer
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder.Ops
import com.truecar.mleap.runtime.types.StringType

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
