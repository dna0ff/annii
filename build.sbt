name := "annii"

version := "0.0.4"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.kudu" %% "kudu-spark3" % "1.14.0" % "provided",
  "io.projectglow" %% "glow-spark3" % "1.0.0" % "provided",
  "com.twitter" %% "chill" % "0.9.5",
  "org.rogach" %% "scallop" % "3.5.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}

publishArtifact in (Compile, packageDoc) := false
publishArtifact in (Compile, packageSrc) := false
packageBin in Compile := (assembly in Compile).value
publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

scalacOptions in (Compile,doc) ++= Seq("-doc-root-content", "docs/rootdoc.txt")