organization := "com.github.losizm"
name         := "little-sql"
version      := "6.0.0-SNAPSHOT"
description  := "The Scala library that provides extension methods for java.sql"
homepage     := Some(url("https://github.com/losizm/little-sql"))
licenses     := List("Apache License, Version 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

versionScheme := Some("early-semver")

scalaVersion := "3.1.2"

scalacOptions := Seq("-deprecation", "-feature", "-new-syntax", "-Werror", "-Yno-experimental")

Compile / doc / scalacOptions := Seq(
  "-project", name.value,
  "-project-version", version.value
)

libraryDependencies ++= Seq(
  "com.h2database" %  "h2"        % "1.4.199" % "test",
  "org.scalatest"  %% "scalatest" % "3.2.12"  % "test"
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
