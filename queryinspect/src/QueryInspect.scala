package com.ajkaanbal.queryinspect

import scala.io.Source
import java.nio.file.Path
import scala.collection.JavaConverters._

import com.alibaba.druid.util.JdbcConstants
import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.parser.SQLParserUtils
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement
import com.alibaba.druid.sql.dialect.hive.ast.HiveInsertStatement

case class Column(name: String, table: String, fullName: String)

case class Stats(
  selectItems: Map[String, String],
  tables: Map[String,String],
  columns: Seq[Column],
  functions: Seq[String],
  conditions: Seq[String],
  relationships: Seq[String]
)
object QueryInspect {
  private def jdbcConstant(dialect: DBType) = dialect match {
    case DBType.ORACLE => JdbcConstants.ORACLE
    case DBType.HIVE => JdbcConstants.HIVE
  }
  def parse(path: Path, dialect: DBType):Stats = {
    val sql = Source.fromFile(path.toFile).mkString.replaceAll("\\$\\{[^}]*\\}","666").replaceAll("overwrite j", "overwrite table j")
    val dbType = jdbcConstant(dialect)
    val parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
    val stmtList = parser.parseStatementList()
    val stmt = stmtList.asScala.headOption;
    val statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
    stmt.map(_.accept(statVisitor))
    val insertStmt = stmt.map(_.asInstanceOf[HiveInsertStatement])
    val queryBlock = insertStmt.map(_.getQuery.getQueryBlock)
    val selectItems = queryBlock.map(q => 
        q.getSelectList
          .asScala
          .map(x => (Option(x.getAlias).getOrElse(""), x.toString)).toMap)
          .getOrElse(Map.empty[String, String])

    val tables = statVisitor.getTables.asScala.map(t => (t._1.getName, t._2.toString)).toMap
    val columns = statVisitor.getColumns.asScala.map(c => Column(c.getName, c.getTable, c.getFullName)).toSeq
    val functions = statVisitor.getFunctions.asScala.map(_.toString).toSeq
    val conditions = statVisitor.getConditions.asScala.map(_.toString).toSeq
    val relationships = statVisitor.getRelationships.asScala.map(_.toString).toSeq
    Stats(selectItems, tables, columns, functions, conditions, relationships)
  }
}
