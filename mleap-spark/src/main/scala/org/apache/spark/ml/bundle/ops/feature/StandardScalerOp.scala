package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object StandardScalerOp extends OpNode[StandardScalerModel, StandardScalerModel] {
  override val Model: OpModel[StandardScalerModel] = new OpModel[StandardScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(context: BundleContext, model: WritableModel, obj: StandardScalerModel): Unit = {
      if(obj.getWithMean) { model.withAttr(Attribute.tensor("mean", Tensor.doubleVector(obj.mean.toArray.toSeq))) }
      if(obj.getWithStd) { model.withAttr(Attribute.tensor("std", Tensor.doubleVector(obj.std.toArray.toSeq))) }
    }

    override def load(context: BundleContext, model: ReadableModel): StandardScalerModel = {
      val std = model.getAttr("std").map(_.getTensor.getDoubleVector.toArray).map(Vectors.dense).orNull
      val mean = model.getAttr("mean").map(_.getTensor.getDoubleVector.toArray).map(Vectors.dense).orNull
      new StandardScalerModel(uid = "", std = std, mean = mean)
    }
  }

  override def name(node: StandardScalerModel): String = node.uid

  override def model(node: StandardScalerModel): StandardScalerModel = node

  override def load(context: BundleContext, node: ReadableNode, model: StandardScalerModel): StandardScalerModel = {
    new StandardScalerModel(uid = node.name, std = model.std, mean = model.mean)
  }

  override def shape(node: StandardScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
