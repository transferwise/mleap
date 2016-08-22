package org.apache.spark.ml.bundle.ops

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.{AttributeBuilder, ShapeBuilder}
import ml.bundle.serializer._
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 8/21/16.
  */
object PipelineOp extends NodeOp[PipelineModel] {
  override def opName: String = Bundle.BuiltinOps.pipeline

  override def name(obj: PipelineModel): String = obj.uid

  override def shape(obj: PipelineModel, registry: OpRegistry): Shape = {
    val sb = ShapeBuilder()
    for(stage <- obj.stages) {
      val op = registry.opForAny(stage)
      sb.withShape(op.shapeAny(stage, registry))
    }
    sb.build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: PipelineModel): Unit = {
    val children = GraphSerializer(writer.path, writer.registry).write(obj.stages)
    writer.withAttribute(ab.stringList("nodes", children))
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): PipelineModel = {
    val children = node.attr("nodes").getStringList
    val stages = GraphSerializer(reader.path, reader.registry).read(children).map(_.asInstanceOf[Transformer])
    new PipelineModel(uid = node.name,
      stages = stages.toArray  )
  }
}
