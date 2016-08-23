package com.truecar.mleap.runtime.serialization.bundle.ops.classification

import com.truecar.mleap.classification.{DecisionTreeClassifierModel, RandomForestClassifierModel}
import com.truecar.mleap.runtime.serialization.bundle.tree.MleapNodeWrapper
import com.truecar.mleap.runtime.transformer.classification.RandomForestClassifier
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.wrapper._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestClassifierOp extends OpNode[RandomForestClassifier, RandomForestClassifierModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[RandomForestClassifierModel] = new OpModel[RandomForestClassifierModel] {
    override def opName: String = Bundle.BuiltinOps.classification.random_forest_classifier

    override def store(context: BundleContext, model: WritableModel, obj: RandomForestClassifierModel): Unit = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.context(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr(Attribute.long("num_features", obj.numFeatures)).
        withAttr(Attribute.long("num_classes", obj.numClasses)).
        withAttr(Attribute.stringList("trees", trees))
    }

    override def load(context: BundleContext, model: ReadableModel): RandomForestClassifierModel = {
      val numFeatures = model.attr("num_features").getLong.toInt
      val numClasses = model.attr("num_classes").getLong.toInt

      val models = model.attr("trees").getStringList.map {
        tree => ModelSerializer(context.context(tree)).read().asInstanceOf[DecisionTreeClassifierModel]
      }

      RandomForestClassifierModel(numFeatures = numFeatures,
        numClasses = numClasses,
        trees = models)
    }
  }

  override def name(node: RandomForestClassifier): String = node.uid

  override def model(node: RandomForestClassifier): RandomForestClassifierModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: RandomForestClassifierModel): RandomForestClassifier = {
    RandomForestClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.input("prediction").name,
      model = model)
  }

  override def shape(node: RandomForestClassifier): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
