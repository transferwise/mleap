package com.truecar.mleap.runtime.serialization.bundle.ops.regression

import com.truecar.mleap.regression.DecisionTreeRegressionModel
import com.truecar.mleap.tree.Node
import com.truecar.mleap.runtime.serialization.bundle.tree.MleapNodeWrapper
import com.truecar.mleap.runtime.transformer.regression.DecisionTreeRegression
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.tree.TreeSerializer
import ml.bundle.wrapper._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object DecisionTreeRegressionOp extends OpNode[DecisionTreeRegression, DecisionTreeRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[DecisionTreeRegressionModel] = new OpModel[DecisionTreeRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(context: BundleContext, model: WritableModel, obj: DecisionTreeRegressionModel): Unit = {
      model.withAttr(Attribute.long("num_features", obj.numFeatures))
      TreeSerializer[Node](context.file("nodes"), withImpurities = false).write(obj.rootNode)
    }

    override def load(context: BundleContext, model: ReadableModel): DecisionTreeRegressionModel = {
      val rootNode = TreeSerializer[Node](context.file("nodes"), withImpurities = false).read()
      DecisionTreeRegressionModel(rootNode, numFeatures = model.attr("num_features").getLong.toInt)
    }
  }

  override def name(node: DecisionTreeRegression): String = node.uid

  override def model(node: DecisionTreeRegression): DecisionTreeRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: DecisionTreeRegressionModel): DecisionTreeRegression = {
    DecisionTreeRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: DecisionTreeRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
