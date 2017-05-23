name := "akka_graph_dsl"

version := "1.0"

scalaVersion := "2.12.2"

resolvers += Resolver.jcenterRepo

libraryDependencies ++= {
  val akkaVersion = "2.5.1"
  val scalaTestVersion = "3.0.1"

  Seq(
    "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
    "ch.qos.logback"    %  "logback-classic"  % "1.0.13",
    "org.mockito"       % "mockito-all" % "1.10.19" % "test",
    "org.scalatest"     %% "scalatest" % scalaTestVersion   % "test",
    "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.5.1.0"
  )
}

fork in Test := true