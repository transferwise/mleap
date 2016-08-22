package org.apache.spark.ml.bundle

import ml.bundle.serializer.BundleRegistry

/**
  * Created by hollinwilkins on 8/21/16.
  */
object SparkRegistry {
  val instance: BundleRegistry = create()

  def create(): BundleRegistry = {
    BundleRegistry().
      // regressions
      register(ops.regression.LinearRegressionOp).

      // classifiers
      register(ops.classification.LogisticRegressionOp).
      register(ops.classification.SupportVectorMachineOp).

      // features
      register(ops.feature.HashingTermFrequencyOp).
      register(ops.feature.OneHotEncoderOp).
      register(ops.feature.ReverseStringIndexerOp).
      register(ops.feature.StandardScalerOp).
      register(ops.feature.StringIndexerOp).
      register(ops.feature.TokenizerOp).
      register(ops.feature.VectorAssemblerOp).

      // other
      register(ops.PipelineOp)
  }
}
