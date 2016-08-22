package org.apache.spark.ml.bundle.ops.classification

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.ShapeBuilder
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object LogisticRegressionOp extends NodeOp[LogisticRegressionModel] {
  override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

  override def name(obj: LogisticRegressionModel): String = obj.uid

  override def shape(obj: LogisticRegressionModel, registry: OpRegistry): Shape = {
    ShapeBuilder().withInput(obj.getFeaturesCol, "features").
      withOutput(obj.getPredictionCol, "prediction").
      build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: LogisticRegressionModel): Unit = {
    writer.withAttribute(ab.tensor("coefficients", tb.doubleVector(obj.coefficients.toArray))).
      withAttribute(ab.double("intercept", obj.intercept))

    for(threshold <- obj.get(obj.threshold)) {
      writer.withAttribute(ab.double("threshold", obj.getThreshold))
    }

    for(thresholds <- obj.get(obj.thresholds)) {
      writer.withAttribute(ab.doubleList("thresholds", thresholds))
    }
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): LogisticRegressionModel = {
    val m = new LogisticRegressionModel(uid = node.name,
      coefficients = Vectors.dense(node.attr("coefficients").getTensor.doubleVal.toArray),
      intercept = node.attr("intercept").getDouble)

    for(threshold <- node.getAttr("threshold")) {
      m.setThreshold(threshold.getDouble)
    }

    for(thresholds <- node.getAttr("thresholds")) {
      m.setThresholds(thresholds.getDoubleList.toArray)
    }

    m
  }
}
