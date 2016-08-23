package org.apache.spark.ml.bundle.ops.classification

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.wrapper._
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestClassifierOp extends OpNode[RandomForestClassificationModel, RandomForestClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[RandomForestClassificationModel] = new OpModel[RandomForestClassificationModel] {
    override def opName: String = Bundle.BuiltinOps.regression.random_forest_regression

    override def store(context: BundleContext, model: WritableModel, obj: RandomForestClassificationModel): Unit = {
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

    override def load(context: BundleContext, model: ReadableModel): RandomForestClassificationModel = {
      val numFeatures = model.attr("num_features").getLong.toInt
      val numClasses = model.attr("num_classes").getLong.toInt

      val models = model.attr("trees").getStringList.map {
        tree => ModelSerializer(context.context(tree)).read().asInstanceOf[DecisionTreeClassificationModel]
      }.toArray

      new RandomForestClassificationModel(uid = "",
        numFeatures = numFeatures,
        numClasses = numClasses,
        _trees = models)
    }
  }

  override def name(node: RandomForestClassificationModel): String = node.uid

  override def model(node: RandomForestClassificationModel): RandomForestClassificationModel = node

  override def load(context: BundleContext, node: ReadableNode, model: RandomForestClassificationModel): RandomForestClassificationModel = {
    new RandomForestClassificationModel(uid = node.name,
      numClasses = model.numClasses,
      numFeatures = model.numFeatures,
      _trees = model.trees).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.input("prediction").name)
  }

  override def shape(node: RandomForestClassificationModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
