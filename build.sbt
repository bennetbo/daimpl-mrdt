ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "testing"
  )

ThisBuild / libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test
ThisBuild / libraryDependencies += "org.scalatestplus" %% "scalacheck-1-17" % "3.2.17.0" % Test
