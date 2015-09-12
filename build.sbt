organization := "org.xerial.msgframe"
sonatypeProfileName := "org.xerial"
description := "MessagePack based DataFrame for Scala"
scalaVersion in Global := "2.11.7"

lazy val core = Project(id = "msgframe-core", base = file("msgframe-core")).settings(
    libraryDependencies ++= Seq(
      "org.msgpack" % "msgpack-core" % "0.7.0-M6",
      "org.scalatest" %% "scalatest" % "2.2.4" % "test"
    )
)

pomExtra in Global := {
    <url>http://xerial.org/msgframe</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/xerial/msgframe.git</connection>
      <developerConnection>scm:git:git@github.com:xerial/msgframe.git</developerConnection>
      <url>github.com/xerial/msgframe.git</url>
    </scm>
    <properties>
      <scala.version>{scalaVersion.value}</scala.version>
      <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <developers>
      <developer>
        <id>leo</id>
        <name>Taro L. Saito</name>
        <url>http://xerial.org/leo</url>
      </developer>
    </developers>
}

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _)),
  setNextVersion,
  commitNextVersion,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
  pushChanges
)
