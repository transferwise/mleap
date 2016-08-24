package com.truecar.mleap.runtime.serialization.bundle.ops.classification

import com.truecar.mleap.classification.{ClassificationModel, OneVsRestModel}
import com.truecar.mleap.runtime.transformer.classification.OneVsRest
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.wrapper._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object OneVsRestOp extends OpNode[OneVsRest, OneVsRestModel] {
  override val Model: OpModel[OneVsRestModel] = new OpModel[OneVsRestModel] {
    override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest

    override def store(context: BundleContext, model: WritableModel, obj: OneVsRestModel): Unit = {
      var i = 0
      for(cModel <- obj.classifiers) {
        val name = s"model$i"
        ModelSerializer(context.context(name)).write(cModel)
        i = i + 1
        name
      }

      model.withAttr(Attribute.long("num_classes", obj.classifiers.length))
    }

    override def load(context: BundleContext, model: ReadableModel): OneVsRestModel = {
      val numClasses = model.attr("num_classes").getLong.toInt

      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.context(s"model$i")).read().asInstanceOf[ClassificationModel]
      }

      OneVsRestModel(classifiers = models)
    }
  }

  override def name(node: OneVsRest): String = node.uid

  override def model(node: OneVsRest): OneVsRestModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: OneVsRestModel): OneVsRest = {
    OneVsRest(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: OneVsRest): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
