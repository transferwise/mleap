package org.apache.spark.ml.bundle

import ml.bundle.serializer.OpRegistry

/**
  * Created by hollinwilkins on 8/21/16.
  */
object SparkRegistry {
  val instance: OpRegistry = create()

  def create(): OpRegistry = {
    OpRegistry().register(ops.regression.LinearRegressionOp).
      register(ops.classification.LogisticRegressionOp).
      register(ops.classification.SupportVectorMachineOp).
      register(ops.feature.HashingTermFrequencyOp).
      register(ops.feature.OneHotEncoderOp).
      register(ops.feature.ReverseStringIndexerOp).
      register(ops.feature.StandardScalerOp).
      register(ops.feature.StringIndexerOp).
      register(ops.feature.TokenizerOp).
      register(ops.feature.VectorAssemblerOp).
      register(ops.feature.StringIndexerOp).
      register(ops.PipelineOp)
  }
}
