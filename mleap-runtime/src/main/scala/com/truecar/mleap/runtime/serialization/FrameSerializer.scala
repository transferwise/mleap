package com.truecar.mleap.runtime.serialization

import java.io.{InputStream, OutputStream}

import com.truecar.mleap.runtime.LocalLeapFrame
import com.truecar.mleap.runtime.types.StructType

/**
  * Created by hollinwilkins on 8/23/16.
  */
trait FrameSerializer {
  def write(out: OutputStream, frame: LocalLeapFrame): Unit
  def read(in: InputStream): LocalLeapFrame

  def withOptions(options: Map[String, String]): FrameSerializer = this
  def withOption(name: String, value: String): FrameSerializer = this
  def withSchema(schema: StructType): FrameSerializer = this
}
