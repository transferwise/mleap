package ml.combust.mleap.runtime

import java.io.File

import ml.combust.mleap.runtime.bundle.MleapRegistry
import ml.combust.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.bundle.Bundle
import ml.bundle.serializer.{BundleContext, BundleRegistry, BundleSerializer}
import ml.bundle.wrapper.AttributeList

/**
  * Created by hollinwilkins on 8/22/16.
  */
object MleapSupport {
  implicit val mleapRegistry: BundleRegistry = MleapRegistry.instance

  implicit class TransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None)
                         (implicit registry: BundleRegistry): Unit = {
      MleapBundle.writeTransformer(transformer, path, list)(registry)
    }
  }

  object MleapBundle {
    def readTransformerGraph(path: File)
                            (implicit registry: BundleRegistry): Pipeline = {
      val bundle = BundleSerializer(BundleContext(registry, path)).read()
      Pipeline(uid = bundle.info.name, transformers = bundle.nodes.map(_.asInstanceOf[Transformer]))
    }

    def readTransformer(path: File)
                       (implicit registry: BundleRegistry): Transformer = {
      val bundle = BundleSerializer(BundleContext(registry, path)).read()
      if(bundle.nodes.length == 1) {
        bundle.nodes.head.asInstanceOf[Transformer]
      } else {
        Pipeline(uid = bundle.info.name, transformers = bundle.nodes.map(_.asInstanceOf[Transformer]))
      }
    }

    def writeTransformerGraph(graph: Pipeline,
                              path: File,
                              list: Option[AttributeList] = None)
                             (implicit registry: BundleRegistry): Unit = {
      val bundle = Bundle.createBundle(graph.uid, graph.transformers, list)
      BundleSerializer(BundleContext(registry, path)).write(bundle)
    }

    def writeTransformer(transformer: Transformer,
                         path: File,
                         list: Option[AttributeList] = None)
                        (implicit registry: BundleRegistry): Unit = {
      transformer match {
        case transformer: Pipeline => writeTransformerGraph(transformer, path, list)(registry)
        case _ =>
          val bundle = Bundle.createBundle(transformer.uid, Seq(transformer), list)
          BundleSerializer(BundleContext(registry, path)).write(bundle)
      }
    }
  }
}
