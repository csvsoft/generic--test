name := "spark-generic-test"
version := "0.1.0"
scalaVersion := "2.12.20"

// Add JVM options for Java 17 compatibility
javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  // Add security-related options
  "--add-opens=java.base/java.security=ALL-UNNAMED",
  "--add-opens=java.base/java.security.auth=ALL-UNNAMED",
  "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
  // Add memory-related options
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.buffer=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.ref=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true",
  "-Djava.security.manager=allow"
)

// Fork the JVM to apply JVM options
fork := true

// Use Spark 3.5.0 which has better Java 17 support
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.2.10",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  
  // Add Cassandra connector dependency
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1",
  
  // Add H2 database dependency
  "com.h2database" % "h2" % "2.2.224",
  
  // Add Spark JDBC dependency for database connectivity
  "org.apache.spark" %% "spark-jdbc" % "3.5.0" % "provided"
)
