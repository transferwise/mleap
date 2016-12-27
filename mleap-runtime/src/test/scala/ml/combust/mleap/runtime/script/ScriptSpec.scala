package ml.combust.mleap.runtime.script

import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.transformer.script.{Script, ScriptModel}
import ml.combust.mleap.runtime.types.{DoubleType, StructField, StructType, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/26/16.
  */
class ScriptSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_val1", DoubleType),
    StructField("test_val2", DoubleType),
    StructField("test_val3", TensorType.doubleVector()))).get
  val dataset = LocalDataset(Seq(Row(54.0, 32.4, Vectors.dense(Array(0.3, 0.77)))))
  val frame = LeapFrame(schema, dataset)

  val js =
    """
      |var test_fun = function(v1, v2, v3) {
      |  return v1 + v2 + v3[1];
      |};
    """.stripMargin
  val model = ScriptModel(ScriptModel.js,
    Seq(DoubleType, DoubleType, TensorType.doubleVector()),
    DoubleType,
    "test_fun",
    js)
  val transformer = Script(inputCols = Seq("test_val1", "test_val2", "test_val3"),
    outputCol = "test_out",
    model = model)

  describe("#transform") {
    it("uses the underlying script and engine to transform the LeapFrame") {
      val frame2 = transformer.transform(frame).get
      val data = frame2.dataset

      assert(data(0).getDouble(3) == (54 + 32.4 + 0.77))
    }
  }
}
