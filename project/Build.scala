import sbt.Keys._
import sbt._
import bintray.Opts
import bintray.Plugin.bintraySettings
import bintray.Keys._
import sbtassembly.{MergeStrategy, PathList, AssemblyKeys}
import AssemblyKeys._

class Build extends sbt.Build {

  protected val bintrayPublishIvyStyle = settingKey[Boolean]("=== !publishMavenStyle") //workaround for sbt-bintray bug

  lazy val publishSettings: Seq[Setting[_]] =  bintraySettings ++ Seq(
    repository in bintray :=  "scalax-releases",

    bintrayOrganization in bintray := Some("scalax"),

    licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0")),

    bintrayPublishIvyStyle := true
  )



  lazy val noPublishSettings: Seq[Setting[_]] = Seq(
    publish := (),
    publishLocal := (),
    publishArtifact := false
  )



  override def rootProject = Some(genesExpressions)


  lazy val genesExpressions = Project(
    id = "gene-expressions",

    base = file(".")
  )
}