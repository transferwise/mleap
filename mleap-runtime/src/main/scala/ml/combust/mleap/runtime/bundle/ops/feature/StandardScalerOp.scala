package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.mleap.feature.StandardScalerModel
import ml.combust.mleap.runtime.transformer.feature.StandardScaler
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object StandardScalerOp extends OpNode[StandardScaler, StandardScalerModel] {
  override val Model: OpModel[StandardScalerModel] = new OpModel[StandardScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(context: BundleContext, model: WritableModel, obj: StandardScalerModel): Unit = {
      for(mean <- obj.mean) { model.withAttr(Attribute.tensor("mean", Tensor.doubleVector(mean.toArray))) }
      for(std <- obj.std) { model.withAttr(Attribute.tensor("std", Tensor.doubleVector(std.toArray))) }
    }

    override def load(context: BundleContext, model: ReadableModel): StandardScalerModel = {
      val mean = model.getAttr("mean").map(_.getTensor.getDoubleVector.toArray).map(Vectors.dense)
      val std = model.getAttr("std").map(_.getTensor.getDoubleVector.toArray).map(Vectors.dense)
      StandardScalerModel(mean = mean, std = std)
    }
  }

  override def name(node: StandardScaler): String = node.uid

  override def model(node: StandardScaler): StandardScalerModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: StandardScalerModel): StandardScaler = {
    StandardScaler(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StandardScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
