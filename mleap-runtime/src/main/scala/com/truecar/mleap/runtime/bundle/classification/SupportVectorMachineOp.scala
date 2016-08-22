package com.truecar.mleap.runtime.bundle.classification

import com.truecar.mleap.core.classification.SupportVectorMachineModel
import com.truecar.mleap.runtime.transformer.classification.SupportVectorMachine
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper.{Tensor, _}
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object SupportVectorMachineOp extends OpNode[SupportVectorMachine, SupportVectorMachineModel] {
  override val Model: OpModel[SupportVectorMachineModel] = new OpModel[SupportVectorMachineModel] {
    override def opName: String = Bundle.BuiltinOps.classification.support_vector_machine

    override def store(context: BundleContext, model: WritableModel, obj: SupportVectorMachineModel): Unit = {
      model.withAttr(Attribute.tensor("coefficients", Tensor.doubleVector(obj.coefficients.toArray.toSeq))).
        withAttr(Attribute.double("intercept", obj.intercept))
    }

    override def load(context: BundleContext, model: ReadableModel): SupportVectorMachineModel = {
      SupportVectorMachineModel(coefficients = Vectors.dense(model.attr("coefficients").getTensor.getDoubleVector.toArray),
        intercept = model.attr("intercept").getDouble)
    }
  }

  override def name(node: SupportVectorMachine): String = node.uid

  override def model(node: SupportVectorMachine): SupportVectorMachineModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: SupportVectorMachineModel): SupportVectorMachine = {
    SupportVectorMachine(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: SupportVectorMachine): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
