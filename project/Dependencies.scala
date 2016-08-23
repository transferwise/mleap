import sbt._

case class Dependencies(version: String) {
  val sparkVersion = "2.0.0"
  val bundleMlVersion = version

  lazy val baseDependencies = Seq("org.scalatest" %% "scalatest" % "3.0.0" % "test")

  lazy val sparkDependencies = Seq("org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-catalyst" % sparkVersion).map(_ % "provided")

  lazy val bundleMlDependencies = Seq("ml.bundle" %% "bundle-ml" % bundleMlVersion)

  lazy val mleapCoreDependencies = baseDependencies.union(Seq("org.apache.spark" %% "spark-mllib-local" % sparkVersion))

  lazy val mleapRuntimeDependencies = bundleMlDependencies

  lazy val mleapSerializationDependencies = bundleMlDependencies

  lazy val mleapSparkDependencies = sparkDependencies
    .union(bundleMlDependencies)

  lazy val mleapCsvDependencies = Seq("com.univocity" % "univocity-parsers" % "2.2.1")
}