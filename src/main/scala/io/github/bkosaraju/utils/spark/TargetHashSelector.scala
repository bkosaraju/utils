/*
 *   Copyright (C) 2019-2020 bkosaraju
 *   All Rights Reserved.
 *
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package io.github.bkosaraju.utils.spark

import io.github.bkosaraju.utils.common.StringToMap
import io.github.bkosaraju.utils.snowflake.ReadSFTable
import io.github.bkosaraju.utils.common.StringToMap
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path

import scala.language.existentials

trait TargetHashSelector extends Context with StringToMap with DataHashGenerator {
val sps =  sparkSession
  import sps.implicits._
  def targetHashSelector(config: Map[String,String]): DataFrame = {
    val readOptions =
      if (Seq("jdbc","rdbms").contains(config.getOrElse("writerFormat","jdbc").trim.toLowerCase)) {
        Map("format" -> "jdbc","dbtable" -> (config("targetDatabase") + "." + config("targetTable"))) ++ config.getOrElse("writerOptions","").stringToMap
      }
      else if (Seq("snowflake","net.snowflake.spark.snowflake").contains(config.getOrElse("writerFormat","").toLowerCase())) {
        var tOps: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]() ++ config.getOrElse("writerOptions", "").stringToMap

        tOps.put("sfDatabase",
          config.getOrElse("database",
            config.getOrElse("targetDatabase",
                config.getOrElse("sfDatabase",
                  config.getOrElse("targetSfDatabase","UN_KNOWN_DB"
                  )
                )
            )
          ).trim
        )

        tOps.put("sfWarehouse",
          config.getOrElse("warehouse",
            config.getOrElse("targetWarehouse",
              config.getOrElse("sfWarehouse",
                config.getOrElse("targetSfWarehouse","UN_KNOWN_WH")
              )
            )
          ).trim
        )

        tOps.put("dbtable",
          config.getOrElse("table",
            config.getOrElse("targetTable",
              config.getOrElse("dbtable",
                config.getOrElse("targetSfTable","UN_KNOWN_TBL")
              )
            )
          ).trim
        )

        tOps.put("sfSchema",
          config.getOrElse("schema",
            config.getOrElse("targetSchema",
              config.getOrElse("sfSchema",
                config.getOrElse("targetSfSchema","UN_KNOWN_SCHEMA")
              )
            )
          ).trim
        )
        tOps
      }
      else {
        Map()
      }
    var targetConfiguration = collection.mutable.Map[String,String]() ++ config
    targetConfiguration = targetConfiguration ++ readOptions
    //Wireup : SF reject the connection in case if same parameters passed to connection string
    targetConfiguration.remove("table")
    targetConfiguration.remove("warehouse")
    targetConfiguration.remove("database")
    targetConfiguration.remove("schema")

    val targetData =
      if (Seq("jdbc","rdbms").contains(config.getOrElse("writerFormat","jdbc").trim.toLowerCase)) {
        sparkSession.read.format("jdbc")
          .options(targetConfiguration).load()
      } else if (Seq("snowflake","net.snowflake.spark.snowflake").contains(config.getOrElse("writerFormat","").toLowerCase()))   {
        sparkSession
          .read
          .format("net.snowflake.spark.snowflake")
          .options(targetConfiguration.-("dbtable"))
          .option("dbtable",targetConfiguration("dbtable")).load()
      }
      else {
        if (hadoopfs.exists(new Path(targetConfiguration("targetURI")))) {
          sparkSession.read
            .format(targetConfiguration.getOrElse("writerFormat", "orc"))
            .options(targetConfiguration)
            .load(targetConfiguration("targetURI"))
        } else {
          Seq.empty[(String,String)].toDF(Seq(
            targetConfiguration.getOrElse("hashedKeyColumns","NonExistedColumn"),
            targetConfiguration.getOrElse("hashedColumn", "row_hash")
          ):_*)
        }
      }

    targetData.createOrReplaceTempView("tgtHashData")

    if (config.contains("hashedKeyColumns")) {
      if (config.contains("hashTargetOrderQualifier")) {
        sparkSession.sql(
          s"""select
             |${config("hashedKeyColumns")},
             |${config.getOrElse("hashedColumn", "row_hash")},
             |ROW_NUMBER() OVER (PARTITION BY ${config("hashedKeyColumns")} ORDER BY ${config("hashTargetOrderQualifier")}) AS rowNumber
             |from tgtHashData
             |""".stripMargin)
          .filter("rowNumber=1")
          .drop("rowNumber")
      } else {
        // if there is no order just select all the values in target
        if (targetData.columns.map(_.toLowerCase())
          .contains(config.getOrElse("hashedColumn", "row_hash").toLowerCase())) {
          val selectableCols = s"""${config("hashedKeyColumns")},${config.getOrElse("hashedColumn", "row_hash")}""".split(",")
          targetData.select(selectableCols.head, selectableCols.tail :_*)
        } else {
          //incase if these key ecolumn not existed in target then generate for the user requirement
          targetData
            .genHash(config("hashedKeyColumns").split(","),
              config.getOrElse("hashedColumn", "row_hash"))
        }
      }
    } else {
      if (config.contains("hashTargetOrderQualifier")) {
        sparkSession.sql(
          s"""select
             |${config.getOrElse("hashedColumn", "row_hash")},
             |ROW_NUMBER() OVER (PARTITION BY ${config.getOrElse("hashedColumn", "row_hash")} ORDER BY ${config("hashTargetOrderQualifier")}) AS rowNumber
             |from tgtHashData
             |""".stripMargin)
          .filter("rowNumber=1")
          .drop("rowNumber")
      } else if (
        //Row Hash column is defined and already available in target table select with out any qualifier
        targetData.columns.map(_.toLowerCase())
        .contains(config.getOrElse("hashedColumn", "row_hash"))){
        targetData.select(config.getOrElse("hashedColumn", "row_hash"))
      } else {
        //Finally If hashed columns are not defined in table then generate each row-hash
        targetData
          .genHash(targetData.columns,
            config.getOrElse("hashedColumn", "row_hash"))
      }
    }
  }
}

