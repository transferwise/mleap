package org.apache.spark.ml.bundle.ops.classification

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.wrapper._
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.{ClassificationModel, OneVsRestModel}

/**
  * Created by hollinwilkins on 8/21/16.
  */
object OneVsRestOp extends OpNode[OneVsRestModel, OneVsRestModel] {

  //  override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest
  //
  //  override def name(obj: OneVsRestModel): String = obj.uid
  //
  //  override def shape(obj: OneVsRestModel, registry: BundleRegistry): Shape = {
  //    ShapeBuilder().withInput(obj.getFeaturesCol, "features").
  //      withOutput(obj.getPredictionCol, "prediction").
  //      build()
  //  }
  //
  //  override def writeAttributes(writer: AttributeWriter, obj: OneVsRestModel): Unit = {
  //    val models = obj.models.map {
  //      model =>
  //        ModelSerializer(new File(writer.path, model.uid), writer.registry).write(model)
  //        model.uid
  //    }
  //
  //    writer.withAttribute(ab.stringList("models", models))
  //  }
  //
  //  override def read(reader: NodeReader, node: NodeDefWrapper): OneVsRestModel = {
  //    val models = node.attr("models").getStringList.map {
  //      model =>
  //        ModelSerializer(new File(reader.path, model), reader.registry).read().asInstanceOf[ClassificationModel[_, _]]
  //    }
  //
  //    val labelMetadata = NominalAttribute.defaultAttr.
  //      withName(node.output("prediction").name).
  //      withNumValues(models.size).
  //      toMetadata
  //    new OneVsRestModel(uid = node.name,
  //      models = models.toArray,
  //      labelMetadata = labelMetadata)
  //  }
  override val Model: OpModel[OneVsRestModel] = new OpModel[OneVsRestModel] {
    override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest

    override def store(context: BundleContext, model: WritableModel, obj: OneVsRestModel): Unit = {
      var i = 0
      for(cModel <- obj.models) {
        val name = s"model$i"
        ModelSerializer(context.context(name)).write(cModel)
        i = i + 1
        name
      }

      model.withAttr(Attribute.long("num_classes", obj.models.length))
    }

    override def load(context: BundleContext, model: ReadableModel): OneVsRestModel = {
      val numClasses = model.attr("num_classes").getLong.toInt

      val models = (0 to numClasses).toArray.map {
        i => ModelSerializer(context.context(s"model$i")).read().asInstanceOf[ClassificationModel[_, _]]
      }

      val labelMetadata = NominalAttribute.defaultAttr.
        withName("prediction").
        withNumValues(models.length).
        toMetadata
      new OneVsRestModel(uid = "", models = models, labelMetadata = labelMetadata)
    }
  }

  override def name(node: OneVsRestModel): String = node.uid

  override def model(node: OneVsRestModel): OneVsRestModel = node

  override def load(context: BundleContext, node: ReadableNode, model: OneVsRestModel): OneVsRestModel = {
    val labelMetadata = NominalAttribute.defaultAttr.
      withName(node.shape.output("prediction").name).
      withNumValues(model.models.length).
      toMetadata
    new OneVsRestModel(uid = node.name, models = model.models, labelMetadata = labelMetadata)
  }

  override def shape(node: OneVsRestModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
