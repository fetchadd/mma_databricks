// Databricks notebook source
object MMATask {
  import upickle.default._
  def run(jsonArgs: String): Unit = {
    val taskArgs = MMATaskArgs.parse(jsonArgs)

    spark.conf.set("odps.end.point", taskArgs.odpsConfig.odpsEndpoint)
    spark.conf.set("odps.project.name", taskArgs.odpsConfig.odpsQuotaProject)
    spark.conf.set("odps.access.id", taskArgs.odpsConfig.odpsAccessId)
    spark.conf.set("odps.access.key", taskArgs.odpsConfig.odpsAccessKey)
    spark.conf.set("odps.tunnel.quota.name", taskArgs.odpsConfig.odpsTunnelQuota)

    val DbArgs(catalog, schema, table, whereCondition, partitions) = taskArgs.dbArgs
    var OdpsArgs(odpsProject, odpsSchema, odpsTable) = taskArgs.odpsArgs

    // read data from databricks table
    val sql = genSelectSql(catalog, schema, table, whereCondition)
    var sqlDf = spark.sql(sql)

    // write data to odps
    if (partitions.nonEmpty) {
      sqlDf = sqlDf.sortWithinPartitions(partitions.head, partitions.slice(1, partitions.length): _*)
    }

    sqlDf.write
      .format("org.apache.spark.sql.execution.datasources.v2.odps")
      .option("project", odpsProject)
      .option("schema", odpsSchema)
      .option("table", odpsTable)
      .mode("append").save
  }

  private def genSelectSql(srcCatalog: String, srcSchema: String, srcTable: String, whereCondition: String): String = {
    if (stringNonEmpty(whereCondition)) {
      s"select * from ${srcCatalog}.${srcSchema}.${srcTable} where ${whereCondition}"
    } else {
      s"select * from ${srcCatalog}.${srcSchema}.${srcTable}"
    }
  }

  private def stringNonEmpty(value: String): Boolean = {
    value != null && value.nonEmpty
  }

  case class MMATaskArgs(dbArgs: DbArgs, odpsConfig: OdpsConfig, odpsArgs: OdpsArgs)

  object MMATaskArgs {
    implicit val rw: ReadWriter[MMATaskArgs] = macroRW

    def parse(json: String): MMATaskArgs = {
      read[MMATaskArgs](json)
    }
  }

  case class DbArgs(catalog: String, schema: String, table: String, whereCondition: String, partitions: List[String])

  object DbArgs {
    implicit val rw: ReadWriter[DbArgs] = macroRW
  }

  case class OdpsConfig(odpsEndpoint: String, odpsQuotaProject: String, odpsTunnelQuota: String, odpsAccessId: String, odpsAccessKey: String)

  object OdpsConfig {
    implicit val rw: ReadWriter[OdpsConfig] = macroRW
  }

  case class OdpsArgs(project: String, var schema: String, var table: String)

  object OdpsArgs {
    implicit val rw: ReadWriter[OdpsArgs] = macroRW
  }
}

val args = dbutils.widgets.get("args")
MMATask.run(args)