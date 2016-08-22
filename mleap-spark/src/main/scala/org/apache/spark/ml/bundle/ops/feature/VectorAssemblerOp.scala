package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.ShapeBuilder
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.feature.VectorAssembler

/**
  * Created by hollinwilkins on 8/21/16.
  */
object VectorAssemblerOp extends NodeOp[VectorAssembler] {
  override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

  override def name(obj: VectorAssembler): String = obj.uid

  override def shape(obj: VectorAssembler, registry: OpRegistry): Shape = {
    val sb = ShapeBuilder().withStandardOut(obj.getOutputCol)
    var i = 0
    for(input <- obj.getInputCols) {
      sb.withInput(input, s"input$i")
      i = i + 1
    }
    sb.build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: VectorAssembler): Unit = { }

  override def read(reader: NodeReader, node: NodeDefWrapper): VectorAssembler = {
    val inputCols = node.nodeDef.shape.inputs.map(_.name)
    new VectorAssembler(uid = node.name).
      setInputCols(inputCols.toArray).
      setOutputCol(node.standardOutput.name)
  }

}
