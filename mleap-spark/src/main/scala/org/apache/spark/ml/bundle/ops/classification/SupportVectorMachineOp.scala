package org.apache.spark.ml.bundle.ops.classification

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.ShapeBuilder
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.mleap.classification.SVMModel
import org.apache.spark.mllib.classification
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object SupportVectorMachineOp extends NodeOp[SVMModel] {
  override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

  override def name(obj: SVMModel): String = obj.uid

  override def shape(obj: SVMModel, registry: OpRegistry): Shape = {
    ShapeBuilder().withInput(obj.getFeaturesCol, "features").
      withOutput(obj.getPredictionCol, "prediction").
      build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: SVMModel): Unit = {
    writer.withAttribute(ab.tensor("coefficients", tb.doubleVector(obj.model.weights.toArray))).
      withAttribute(ab.double("intercept", obj.model.intercept))

    for(threshold <- obj.model.getThreshold) {
      writer.withAttribute(ab.double("threshold", threshold))
    }
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): SVMModel = {
    val model = new classification.SVMModel(weights = Vectors.dense(node.attr("coefficients").getTensor.doubleVal.toArray),
      intercept = node.attr("intercept").getDouble)

    new SVMModel(uid = node.name, model = model).
      setThreshold(node.getAttr("threshold").map(_.getDouble)).
      setFeaturesCol(node.input("features").name).
      setPredictionCol(node.output("prediction").name)
  }

}
