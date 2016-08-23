package com.truecar.mleap.runtime.transformer.builder

import com.truecar.mleap.runtime.types.{DataType, StructField}
import com.truecar.mleap.runtime.{LeapFrame, Row}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
case class LeapFrameBuilder[T: LeapFrame](frame: T) extends Serializable
object LeapFrameBuilder {
  implicit def LeapFrameBuilderTransformBuilder[T: LeapFrame]: TransformBuilder[LeapFrameBuilder[T]] = {
    new TransformBuilder[LeapFrameBuilder[T]] {
      override def withInput(t: LeapFrameBuilder[T], name: String): Try[(LeapFrameBuilder[T], Int)] = {
        LeapFrame.schema(t.frame).tryIndexOf(name).map((t, _))
      }

      override def withInput(t: LeapFrameBuilder[T], name: String, dataType: DataType): Try[(LeapFrameBuilder[T], Int)] = {
        val schema = LeapFrame.schema(t.frame)
        schema.getField(name) match {
          case Some(field) =>
            if(field.dataType.fits(dataType)) {
              Success(t, schema.indexOf(name))
            } else {
              Failure(new Error(s"Field $name expected data type ${field.dataType} but found $dataType"))
            }
          case None =>
            Failure(new Error(s"Field $name does not exist"))
        }
      }

      override def withOutput(t: LeapFrameBuilder[T], name: String, dataType: DataType)(o: (Row) => Any): Try[LeapFrameBuilder[T]] = {
        LeapFrame.withField(t.frame, StructField(name, dataType), o).map {
          frame2 => t.copy(frame = frame2)
        }
      }
    }
  }
}
