import mill._, scalalib._

object queryinspect extends ScalaModule {
  def scalaVersion = "2.12.10"
  def ivyDeps = Agg(
    ivy"com.alibaba:druid:1.1.21",
    ivy"com.monovore::decline:1.0.0",
    ivy"com.monovore::decline-enumeratum:1.0.0",
    ivy"guru.nidi:graphviz-java:0.14.1",
    ivy"com.lihaoyi::os-lib:0.6.2"
  )

}
