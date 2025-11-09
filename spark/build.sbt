name := "spark"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" excludeAll(
    ExclusionRule(organization = "commons-logging", name = "commons-logging")
  ),
  "org.apache.spark" %% "spark-sql"  % "3.5.1" excludeAll(
    ExclusionRule(organization = "commons-logging", name = "commons-logging")
  )
)
// Enable parallel execution
concurrentRestrictions in Global += Tags.limit(Tags.Test, 4)

// Set fork options to improve memory usage
fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx16G")

// Enable cached resolution
updateOptions := updateOptions.value.withCachedResolution(true)


Compile / mainClass := Some("org.cscie88c.spark.SparkJob")

assembly / mainClass := Some("org.cscie88c.spark.SparkJob")
assembly / assemblyJarName := "SparkJob.jar"
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  
  // Handle Arrow conflicts
  case "arrow-git.properties" => MergeStrategy.first
  
  // Handle Google Protobuf conflicts
  case PathList("google", "protobuf", xs @ _*) => MergeStrategy.first
  
  // Handle commons-logging vs jcl-over-slf4j conflicts (prefer slf4j)
  case PathList("org", "apache", "commons", "logging", xs @ _*) => MergeStrategy.first
  
  // Handle other common conflicts
  case "module-info.class" => MergeStrategy.discard
  case "git.properties" => MergeStrategy.first
  case PathList("META-INF", "versions", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "maven", xs @ _*) => MergeStrategy.discard
  
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
// see shading feature at https://github.com/sbt/sbt-assembly#shading
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapeless.@1").inAll
)
