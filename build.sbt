name := "little-sql"
version := "0.2.0-SNAPSHOT"
organization := "losizm"
  
scalaVersion := "2.12.6"

scalacOptions ++= Seq("-deprecation", "-feature", "-Xcheckinit")

libraryDependencies ++= Seq(
  "com.h2database"  %  "h2"        % "1.4.197" % "test",
  "org.scalatest"   %% "scalatest" % "3.0.5"   % "test"
)
