package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.regression.RandomForestRegressionModel
import ml.combust.mleap.runtime.attribute.{AttributeSchema, ContinuousAttribute}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder.Ops
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 11/8/15.
  */
case class RandomForestRegression(uid: String = Transformer.uniqueName("random_forest_regression"),
                                  featuresCol: String,
                                  predictionCol: String,
                                  model: RandomForestRegressionModel) extends Transformer {
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
