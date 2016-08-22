name := "mleap"

updateOptions := updateOptions.value.withCachedResolution(true)

lazy val `root` = project.in(file("."))
  .settings(Common.settings)
  .settings(publishArtifact := false)
  .aggregate(`mleap-core`, `mleap-runtime`, `mleap-spark`, `mleap-package`)

lazy val `mleap-core` = project.in(file("mleap-core"))
  .settings(Common.settings)
  .settings(Common.sonatypeSettings)
  .settings(libraryDependencies ++= Dependencies.mleapCoreDependencies)

lazy val `mleap-runtime` = project.in(file("mleap-runtime"))
  .settings(Common.settings)
  .settings(Common.sonatypeSettings)
  .settings(libraryDependencies ++= Dependencies.mleapRuntimeDependencies)
  .dependsOn(`mleap-core`)

lazy val `mleap-spark` = project.in(file("mleap-spark"))
  .settings(Common.settings)
  .settings(Common.sonatypeSettings)
  .settings(libraryDependencies ++= Dependencies.mleapSparkDependencies)

lazy val `mleap-package` = project
  .settings(Common.settings)
  .settings(Common.sonatypeSettings)
  .dependsOn(`mleap-spark`)
