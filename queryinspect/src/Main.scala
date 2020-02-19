package com.ajkaanbal.queryinspect

import java.nio.file.Path
import scala.collection.JavaConverters._

import cats.implicits._
import com.monovore.decline._
import _root_.enumeratum._
import com.monovore.decline.enumeratum._
import guru.nidi.graphviz.model.Factory._
import guru.nidi.graphviz.model.{Graph, Node}
import guru.nidi.graphviz.engine.{Format, Graphviz}
import _root_.enumeratum.values.StringEnum
import guru.nidi.graphviz.attribute.Style
import guru.nidi.graphviz.attribute.Rank
import guru.nidi.graphviz.attribute.Rank.RankDir.LEFT_TO_RIGHT


sealed trait DBType extends EnumEntry with EnumEntry.Lowercase

object DBType extends Enum[DBType] {
  case object ORACLE extends DBType
  case object HIVE extends DBType
  val values = findValues
}


object Main extends CommandApp(name="query-inspect", header="No run, just inpect", main = {
  val queryFilePathValue = Opts.option[Path]("input", "Input file path.")
  val dialectValue = Opts.option[DBType]("dialect", "Query dialect. Supported: 'hive'(default), 'oracle'").withDefault(DBType.HIVE)
  (queryFilePathValue, dialectValue).mapN( (inputPath, dialect) => {
    val stat = QueryInspect.parse(inputPath, dialect)
    val main = node("main")
    val selectItemsNodes = stat
      .selectItems
      .map(i =>
         node(i._1)
          .link(to(node(i._2)).`with`(Style.DASHED))
      ).toList

    val tableNodes = stat
      .tables
      .map{case (name, t) =>
          node(name)
            .link(
              to(node(t))
                .`with`(Style.DASHED)
            )
      }.toList

    val columnNodes = stat
      .columns
      .map{ column =>
        node(column.name).link(to(node(column.table)))
      }
    val nodes = selectItemsNodes ++ tableNodes ++ columnNodes
    val g = graph("test")
      .directed().graphAttr().`with`(Rank.dir(LEFT_TO_RIGHT))
      .`with`(nodes.asJava)

    val id = stat.tables.find(_._2 == "Insert").map(_._1).getOrElse("noinsert")

    val statsReport =
      s"""
      | Select values:
      |  * ${stat.selectItems.map(_._2).mkString("\n  * ")}
      |
      | Tables:
      |  * ${stat.tables.map(_._1).mkString("\n  * ")}
      |
      | Columns:
      |  * ${stat.columns.map(_.fullName).mkString("\n  * ")}
      |
      | Conditions:
      |  * ${stat.conditions.mkString("\n  * ")}
      |
      | Functions:
      |  * ${stat.functions.mkString("\n  * ")}
      |
      | relationships:
      |  * ${stat.relationships.mkString("\n  * ")}
      """.stripMargin
    println(statsReport)
    os.write(os.pwd/s"$id.txt", statsReport)
    Graphviz.fromGraph(g).height(1000).render(Format.PNG).toFile(new java.io.File(s"$id.png"));


  })
})
