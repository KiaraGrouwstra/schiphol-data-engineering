name := "schiphol-kiara"

version := "0.0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core"      % "3.1.1" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"       % "3.1.1" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1" //% "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
