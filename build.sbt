organization := "com.github.losizm"
name         := "little-sql"
version      := "3.0.0-SNAPSHOT"

description  := "The Scala library that provides extension methods to java.sql"
homepage     := Some(url("https://github.com/losizm/little-sql"))
licenses     := List("Apache License, Version 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

scalaVersion := "3.0.2"

scalacOptions ++= Seq("-deprecation", "-feature", "-new-syntax", "-Xfatal-warnings", "-Yno-experimental")

Compile / doc / scalacOptions ++= Seq(
  "-project", name.value,
  "-project-version", {
    val ver = version.value
    ver.substring(0, ver.lastIndexOf(".")) ++ ".x"
  }
)

libraryDependencies ++= Seq(
  "com.h2database" %  "h2"        % "1.4.199" % "test",
  "org.scalatest"  %% "scalatest" % "3.2.9"   % "test"
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/losizm/little-sql"),
    "scm:git@github.com:losizm/little-sql.git"
  )
)

developers := List(
  Developer(
    id    = "losizm",
    name  = "Carlos Conyers",
    email = "carlos.conyers@hotmail.com",
    url   = url("https://github.com/losizm")
  )
)

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org"
  isSnapshot.value match {
    case true  => Some("snaphsots" at s"$nexus/content/repositories/snapshots")
    case false => Some("releases" at s"$nexus/service/local/staging/deploy/maven2")
  }
}

publishMavenStyle := true
