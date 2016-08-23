package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.runtime.types.DataType
import ml.combust.mleap.runtime.Row

import scala.util.{Failure, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
trait TransformBuilder[T] extends Serializable {
  def withInput(t: T, name: String): Try[(T, Int)]
  def withInput(t: T, name: String, dataType: DataType): Try[(T, Int)]

  def withOutput(t: T, name: String, dataType: DataType)
                (o: (Row) => Any): Try[T]
}

object TransformBuilder {
  implicit class Ops[T: TransformBuilder](t: T) {
    def withInput(name: String): Try[(T, Int)] = {
      implicitly[TransformBuilder[T]].withInput(t, name)
    }

    def withInput(name: String, dataType: DataType): Try[(T, Int)] = {
      implicitly[TransformBuilder[T]].withInput(t, name, dataType)
    }

    def withOutput(name: String, dataType: DataType)
                                       (o: (Row) => Any): Try[T] = {
      implicitly[TransformBuilder[T]].withOutput(t, name, dataType)(o)
    }
  }
}
