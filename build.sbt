name := """wikiParser"""

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.3",
  "com.typesafe.akka" %% "akka-agent" % "2.3.3",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.2"
)

//  "com.typesafe.akka" %% "akka-testkit" % "2.3.3",
//  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
//  "junit" % "junit" % "4.11" % "test",
//  "com.novocode" % "junit-interface" % "0.10" % "test"
//)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

