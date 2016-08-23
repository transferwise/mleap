package org.apache.spark.ml.bundle.ops.regression

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.wrapper._
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestRegressionOp extends OpNode[RandomForestRegressionModel, RandomForestRegressionModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[RandomForestRegressionModel] = new OpModel[RandomForestRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.random_forest_regression

    override def store(context: BundleContext, model: WritableModel, obj: RandomForestRegressionModel): Unit = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.context(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr(Attribute.long("num_features", obj.numFeatures)).
        withAttr(Attribute.stringList("trees", trees))
    }

    override def load(context: BundleContext, model: ReadableModel): RandomForestRegressionModel = {
      val numFeatures = model.attr("num_features").getLong.toInt

      val models = model.attr("trees").getStringList.map {
        tree => ModelSerializer(context.context(tree)).read().asInstanceOf[DecisionTreeRegressionModel]
      }.toArray

      new RandomForestRegressionModel(uid = "",
        numFeatures = numFeatures,
        _trees = models)
    }
  }

  override def name(node: RandomForestRegressionModel): String = node.uid

  override def model(node: RandomForestRegressionModel): RandomForestRegressionModel = node

  override def load(context: BundleContext, node: ReadableNode, model: RandomForestRegressionModel): RandomForestRegressionModel = {
    new RandomForestRegressionModel(uid = node.name,
      numFeatures = model.numFeatures,
      _trees = model.trees).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.input("prediction").name)
  }

  override def shape(node: RandomForestRegressionModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
