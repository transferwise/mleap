package org.apache.spark.ml.bundle.ops.classification

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object LogisticRegressionOp extends OpNode[LogisticRegressionModel, LogisticRegressionModel] {
  override val Model: OpModel[LogisticRegressionModel] = new OpModel[LogisticRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(context: BundleContext, model: WritableModel, obj: LogisticRegressionModel): Unit = {
      model.withAttr(Attribute.tensor("coefficients", Tensor.doubleVector(obj.coefficients.toArray.toSeq))).
        withAttr(Attribute.double("intercept", obj.intercept))

      for(t <- obj.get(obj.threshold)) {
        model.withAttr(Attribute.double("threshold", t))
      }
    }

    override def load(context: BundleContext, model: ReadableModel): LogisticRegressionModel = {
      val lr = new LogisticRegressionModel(uid = "",
        coefficients = Vectors.dense(model.attr("coefficients").getTensor.getDoubleVector.toArray),
        intercept = model.attr("intercept").getDouble)

      model.getAttr("threshold").
        map(_.getDouble).
        map(lr.setThreshold).
        getOrElse(lr)
    }
  }

  override def name(node: LogisticRegressionModel): String = node.uid

  override def model(node: LogisticRegressionModel): LogisticRegressionModel = node

  override def load(context: BundleContext, node: ReadableNode, model: LogisticRegressionModel): LogisticRegressionModel = {
    new LogisticRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept).copy(model.extractParamMap).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: LogisticRegressionModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
