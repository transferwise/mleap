package com.truecar.mleap.runtime.transformer.regression

import com.truecar.mleap.core.regression.RandomForestRegression
import com.truecar.mleap.runtime.attribute.{AttributeSchema, ContinuousAttribute}
import com.truecar.mleap.runtime.transformer.Transformer
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder.Ops
import com.truecar.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 11/8/15.
  */
case class RandomForestRegressionModel(uid: String = Transformer.uniqueName("random_forest_regression"),
                                       featuresCol: String,
                                       predictionCol: String,
                                       model: RandomForestRegression) extends Transformer {
  override def build[TB: TransformBuilder](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case (b, featuresIndex) =>
        b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
    }
  }

  override def transformAttributeSchema(schema: AttributeSchema): AttributeSchema = {
    schema.withField(predictionCol, ContinuousAttribute())
  }
}
