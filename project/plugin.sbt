addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.8.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("de.johoop" % "findbugs4sbt" % "1.3.0")
addSbtPlugin("de.johoop" % "jacoco4sbt" % "2.1.6")

scalacOptions ++= Seq("-deprecation", "-feature")
