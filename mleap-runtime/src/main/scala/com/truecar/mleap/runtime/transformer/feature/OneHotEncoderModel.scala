package com.truecar.mleap.runtime.transformer.feature

import com.truecar.mleap.core.feature.OneHotEncoder
import com.truecar.mleap.runtime.attribute.{AttributeGroup, AttributeSchema, BaseAttribute, CategoricalAttribute}
import com.truecar.mleap.runtime.transformer.Transformer
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder.Ops
import com.truecar.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hollinwilkins on 5/10/16.
  */
case class OneHotEncoderModel(uid: String = Transformer.uniqueName("one_hot_encoder"),
                              inputCol: String,
                              outputCol: String,
                              model: OneHotEncoder) extends Transformer {
  override def build[TB: TransformBuilder](builder: TB): Try[TB] = {
    builder.withInput(inputCol, DoubleType).flatMap {
      case(b, index) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => model(row.getDouble(index)))
    }
  }

  override def transformAttributeSchema(schema: AttributeSchema): AttributeSchema = {
    val attrs: Array[BaseAttribute] = Array.tabulate(model.size)(n => CategoricalAttribute())
    val attrGroup = AttributeGroup(attrs)
    schema.withField(outputCol, attrGroup)
  }
}
