package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.runtime.attribute.AttributeSchema
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
 * Created by hwilkins on 11/8/15.
 */
case class Pipeline(uid: String = Transformer.uniqueName("pipeline"),
                    transformers: Seq[Transformer]) extends Transformer {
  override def build[TB: TransformBuilder](builder: TB): Try[TB] = {
    transformers.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.build(_)))
  }

  override def transformAttributeSchema(schema: AttributeSchema): AttributeSchema = {
    transformers.foldLeft(schema)((m, transformer) => transformer.transformAttributeSchema(m))
  }
}
