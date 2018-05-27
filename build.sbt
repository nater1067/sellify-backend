name := "akka-helloworld"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies += "com.typesafe.akka" %% "akka-http"   % "10.1.1"
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.1"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.11" // or whatever the latest version is
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"