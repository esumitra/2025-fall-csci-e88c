name := "kafka"

val kafkaClientVersion = "3.4.0"

libraryDependencies ++= Seq(
    // kafka streams
    "org.apache.kafka" %% "kafka-streams-scala" % kafkaClientVersion,
    "com.goyeau" %% "kafka-streams-circe" % "0.6.3",
)
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2"

Compile / mainClass := Some("org.cscie88c.kafka.StreamingApp")

assembly / mainClass := Some("org.cscie88c.kafka.StreamingApp")
assembly / assemblyJarName := "StreamingApp.jar"
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
// see shading feature at https://github.com/sbt/sbt-assembly#shading
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapeless.@1").inAll
)
