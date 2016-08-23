package ml.combust.mleap.regression

import org.scalatest.FunSpec
import ml.combust.mleap.linalg

/**
  * Created by hwilkins on 1/21/16.
  */
class LinearRegressionSpec extends FunSpec {
  describe("#apply") {
    it("applies the linear regression to a feature vector") {
      val linearRegression = LinearRegressionModel(linalg.Vector.dense(Array(0.5, 0.75, 0.25)), .33)
      assert(linearRegression(linalg.Vector.dense(Array(1.0, 0.5, 1.0))) == 1.455)
    }
  }
}
