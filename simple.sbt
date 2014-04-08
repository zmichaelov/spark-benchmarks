name := "benchmarks.Benchmark Runner"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1"

libraryDependencies += "com.sun.tools.btrace" % "btrace-boot" % "1.2.3"

libraryDependencies += "com.sun.tools.btrace" % "btrace-client" % "1.2.3"

libraryDependencies += "com.sun.tools.btrace" % "btrace-agent" % "1.2.3"

javaOptions += "-Xms2G -Xmx2G"

excludeFilter in unmanagedSources := "KMeansBench.scala"
