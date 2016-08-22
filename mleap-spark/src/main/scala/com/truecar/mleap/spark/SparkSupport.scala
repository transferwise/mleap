package com.truecar.mleap.spark

import java.io.File

import ml.bundle.Bundle
import ml.bundle.serializer.{BundleContext, BundleRegistry, BundleSerializer}
import ml.bundle.wrapper.AttributeList
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkRegistry

/**
  * Created by hollinwilkins on 8/22/16.
  */
trait SparkSupport {
  implicit val registry: BundleRegistry = SparkRegistry.instance

  implicit class TransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          name: String,
                          list: Option[AttributeList] = None)
                         (implicit registry: BundleRegistry): Unit = {
      val bundle = Bundle.createBundle(name, Seq(transformer), list)
      BundleSerializer(BundleContext(registry, path)).write(bundle)
    }
  }
}
object SparkSupport extends SparkSupport
