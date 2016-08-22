package com.truecar.mleap.runtime.transformer.feature

import com.truecar.mleap.core.feature.HashingTermFrequency
import com.truecar.mleap.runtime.attribute.{AttributeGroup, AttributeSchema, CategoricalAttribute}
import com.truecar.mleap.runtime.transformer.Transformer
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder
import com.truecar.mleap.runtime.transformer.builder.TransformBuilder.Ops
import com.truecar.mleap.runtime.types.{StringType, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 12/30/15.
  */
case class HashingTermFrequencyModel(uid: String = Transformer.uniqueName("hashing_term_frequency"),
                                     inputCol: String,
                                     outputCol: String,
                                     hashingTermFrequency: HashingTermFrequency) extends Transformer {
  override def build[TB: TransformBuilder](builder: TB): Try[TB] = {
    builder.withInput(inputCol, StringType).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => hashingTermFrequency(row.getString(inputIndex)))
    }
  }

  override def transformAttributeSchema(schema: AttributeSchema): AttributeSchema = {
    val attrGroup = AttributeGroup(Array.tabulate(hashingTermFrequency.numFeatures)(_ => CategoricalAttribute()))
    schema.withField(outputCol, attrGroup)
  }
}
