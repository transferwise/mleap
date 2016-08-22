package com.truecar.mleap.spark

import java.io.File

import ml.bundle.Bundle
import ml.bundle.serializer.{BundleContext, BundleRegistry, BundleSerializer}
import ml.bundle.wrapper.AttributeList
import org.apache.spark.ml.{PipelineModel, Transformer}
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
      SparkBundle.writeTransformer(transformer, path, name, list)
    }
  }

  object SparkBundle {
    def readTransformerGraph(path: File)
                            (implicit registry: BundleRegistry): PipelineModel = {
      new PipelineModel(BundleSerializer(BundleContext(registry, path)).read().nodes.map(_.asInstanceOf[Transformer]))
    }

    def readTransformer(path: File)
                  (implicit registry: BundleRegistry): Transformer = {
      BundleSerializer(BundleContext(registry, path)).read().nodes.map(_.asInstanceOf[Transformer]).head
    }

    def writeTransformerGraph(graph: PipelineModel,
                              path: File,
                              name: String = "unknown",
                              list: Option[AttributeList] = None): Unit = {
      val bundle = Bundle.createBundle(name, graph.stages, list)
      BundleSerializer(BundleContext(registry, path)).write(bundle)
    }

    def writeTransformer(transformer: Transformer,
                         path: File,
                         name: String = "unknown",
                         list: Option[AttributeList] = None)
                        (implicit registry: BundleRegistry): Unit = {
      val bundle = Bundle.createBundle(name, Seq(transformer), list)
      BundleSerializer(BundleContext(registry, path)).write(bundle)
    }
  }
}
object SparkSupport extends SparkSupport
