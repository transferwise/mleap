package com.truecar.mleap.runtime.serialization.bundle.ops.regression

import com.truecar.mleap.core.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel}
import com.truecar.mleap.runtime.serialization.bundle.tree.MleapNodeWrapper
import com.truecar.mleap.runtime.transformer.regression.RandomForestRegression
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.wrapper._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestRegressionOp extends OpNode[RandomForestRegression, RandomForestRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

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
      }

      RandomForestRegressionModel(numFeatures = numFeatures,
        trees = models)
    }
  }

  override def name(node: RandomForestRegression): String = node.uid

  override def model(node: RandomForestRegression): RandomForestRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: RandomForestRegressionModel): RandomForestRegression = {
    RandomForestRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.input("prediction").name,
      model = model)
  }

  override def shape(node: RandomForestRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
