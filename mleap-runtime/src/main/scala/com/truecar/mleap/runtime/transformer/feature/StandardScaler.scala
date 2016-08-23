package com.truecar.mleap.runtime.transformer.feature

import com.truecar.mleap.feature.StandardScalerModel
import com.truecar.mleap.runtime.attribute.AttributeSchema
import com.truecar.mleap.runtime.transformer.Transformer
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder.Ops
import com.truecar.mleap.runtime.types.TensorType

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class StandardScaler(uid: String = Transformer.uniqueName("standard_scaler"),
                          inputCol: String,
                          outputCol: String,
                          model: StandardScalerModel) extends Transformer {
  override def build[TB: TransformBuilder](builder: TB): Try[TB] = {
    builder.withInput(inputCol, TensorType.doubleVector()).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => model(row.getVector(inputIndex)))
    }
  }

  override def transformAttributeSchema(schema: AttributeSchema): AttributeSchema = {
    schema.withField(outputCol, schema(inputCol))
  }
}
