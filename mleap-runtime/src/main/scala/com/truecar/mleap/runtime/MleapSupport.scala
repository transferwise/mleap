package com.truecar.mleap.runtime

import java.io.File

import com.truecar.mleap.runtime.serialization.bundle.{MleapBundle, MleapRegistry}
import com.truecar.mleap.runtime.transformer.{Pipeline, Transformer}
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
}
