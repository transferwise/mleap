package org.apache.spark.ml.bundle.ops.classification

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.mleap.classification.SVMModel
import org.apache.spark.mllib.classification
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object SupportVectorMachineOp extends OpNode[SVMModel, SVMModel] {
  override val Model: OpModel[SVMModel] = new OpModel[SVMModel] {
    override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

    override def store(context: BundleContext, model: WritableModel, obj: SVMModel): Unit = {
      model.withAttr(Attribute.tensor("coefficients", Tensor.doubleVector(obj.model.weights.toArray.toSeq))).
        withAttr(Attribute.double("intercept", obj.model.intercept)).
        withAttr(Attribute.double("threshold", obj.getThreshold))
    }

    override def load(context: BundleContext, model: ReadableModel): SVMModel = {
      val svm = new classification.SVMModel(weights = Vectors.dense(model.attr("coefficients").getTensor.getDoubleVector.toArray),
        intercept = model.attr("intercept").getDouble)
      new SVMModel(uid = "", model = svm).
        setThreshold(model.attr("threshold").getDouble)
    }
  }

  override def name(node: SVMModel): String = node.uid

  override def model(node: SVMModel): SVMModel = node

  override def load(context: BundleContext, node: ReadableNode, model: SVMModel): SVMModel = {
    new SVMModel(uid = node.name,
      model = model.model).copy(model.extractParamMap()).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: SVMModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
