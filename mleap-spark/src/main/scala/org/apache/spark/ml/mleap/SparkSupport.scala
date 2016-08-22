package org.apache.spark.ml.mleap

import java.io.File

import ml.bundle.Bundle
import ml.bundle.serializer.{BundleContext, BundleRegistry, BundleSerializer}
import ml.bundle.wrapper.AttributeList
import org.apache.spark.ml.bundle.SparkRegistry
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 8/22/16.
  */
trait SparkSupport {
  implicit val registry: BundleRegistry = SparkRegistry.instance

  implicit class TransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None)
                         (implicit registry: BundleRegistry): Unit = {
      SparkBundle.writeTransformer(transformer, path, list)
    }
  }

  object SparkBundle {
    def readTransformerGraph(path: File)
                            (implicit registry: BundleRegistry): PipelineModel = {
      val bundle = BundleSerializer(BundleContext(registry, path)).read()
      new PipelineModel(uid = bundle.info.name, stages = bundle.nodes.map(_.asInstanceOf[Transformer]).toArray)
    }

    def readTransformer(path: File)
                       (implicit registry: BundleRegistry): Transformer = {
      val bundle = BundleSerializer(BundleContext(registry, path)).read()
      if(bundle.nodes.length == 1) {
        bundle.nodes.head.asInstanceOf[Transformer]
      } else {
        new PipelineModel(uid = bundle.info.name, stages = bundle.nodes.map(_.asInstanceOf[Transformer]).toArray)
      }
    }

    def writeTransformerGraph(graph: PipelineModel,
                              path: File,
                              list: Option[AttributeList] = None)
                             (implicit registry: BundleRegistry): Unit = {
      val bundle = Bundle.createBundle(graph.uid, graph.stages, list)
      BundleSerializer(BundleContext(registry, path)).write(bundle)
    }

    def writeTransformer(transformer: Transformer,
                         path: File,
                         list: Option[AttributeList] = None)
                        (implicit registry: BundleRegistry): Unit = {
      transformer match {
        case transformer: PipelineModel => writeTransformerGraph(transformer, path, list)
        case _ =>
          val bundle = Bundle.createBundle(transformer.uid, Seq(transformer), list)
          BundleSerializer(BundleContext(registry, path)).write(bundle)
      }
    }
  }
}
object SparkSupport extends SparkSupport
