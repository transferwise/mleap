package ml.combust.mleap.runtime.transformer.script

import javax.script.{Invocable, ScriptEngineManager}

import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by hollinwilkins on 12/26/16.
  */
object ScriptModel {
  val js = "js"

  val engineForScriptType: Map[String, String] = Map(
    "js" -> "nashorn"
  )

  def inputConverter(dataType: DataType): (AnyRef) => AnyRef = dataType match {
    case dataType: TensorType if dataType.base == DoubleType && dataType.dimensions.size == 1 =>
      (value: AnyRef) => value.asInstanceOf[Vector].toArray
    case dataType: ListType =>
      (value: AnyRef) => value.asInstanceOf[Seq[_]].toArray
    case _ => identity
  }

  def returnConverter(dataType: DataType): (AnyRef) => AnyRef = dataType match {
    case dataType: TensorType if dataType.base == DoubleType && dataType.dimensions.size == 1 =>
      (value: AnyRef) => Vectors.dense(value.asInstanceOf[Array[Double]])
    case dataType: ListType =>
      (value: AnyRef) => value.asInstanceOf[Array[_]].toSeq
    case _ => identity
  }
}

case class ScriptModel(scriptType: String,
                       inputs: Seq[DataType],
                       returnType: DataType,
                       functionName: String,
                       source: String) {
  lazy val invocable = {
    val engine = new ScriptEngineManager().getEngineByName(ScriptModel.engineForScriptType(scriptType))
    engine.eval(source)
    engine.asInstanceOf[Invocable]
  }
  val ic = inputs.map(ScriptModel.inputConverter)
  val rc = ScriptModel.returnConverter(returnType)
  lazy val wrapper = inputs.size match {
    case 0 => () => apply()
    case 1 => (a1: AnyRef) => apply(ic.head(a1))
    case 2 => (a1: AnyRef, a2: AnyRef) => apply(ic.head(a1), ic(1)(a2))
    case 3 => (a1: AnyRef, a2: AnyRef, a3: AnyRef) => apply(ic.head(a1), ic(1)(a2), ic(2)(a3))
    case 4 => (a1: AnyRef, a2: AnyRef, a3: AnyRef, a4: AnyRef) => apply(ic.head(a1), ic(1)(a2), ic(2)(a3), ic(3)(a4))
    case 5 => (a1: AnyRef, a2: AnyRef, a3: AnyRef, a4: AnyRef, a5: AnyRef) => apply(ic.head(a1), ic(1)(a2), ic(2)(a3), ic(3)(a4), ic(4)(a5))
  }
  lazy val udf: UserDefinedFunction = UserDefinedFunction(wrapper, returnType, inputs)

  def apply(args: AnyRef *): AnyRef = {
    rc(invocable.invokeFunction(functionName, args: _*))
  }
}
