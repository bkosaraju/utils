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

package io.github.bkosaraju.utils.snowflake

import io.github.bkosaraju.utils.interface.Common
import io.github.bkosaraju.utils.spark.Context
import io.github.bkosaraju.utils.spark.Context
import org.apache.spark.sql.DataFrame

trait ReadSFTable extends Context with Common {

  def readSFTable(config: Map[String, String]): DataFrame = {
    try {
      var sfConf: collection.mutable.Map[String, String] = collection.mutable.Map()
      val inputSchema = config.getOrElse("schema",
        config.getOrElse("sourceSchema",
          config.getOrElse("sfSchema",
            config.getOrElse("sourceSfSchema",
              "")
          )
        )
      ).trim

      val schemaPrepender = if (inputSchema.nonEmpty) {
        logger.info(s"loading data from schema ${inputSchema}")
         inputSchema + "."
      } else {
        logger.warn("Unable to identify InputSchema deriving from other parameters")
        ""
      }

      //Table Function Read
      val tblFunction = if (config.getOrElse("tableFunction", config.getOrElse("sourceTableFunction", "")).trim.nonEmpty) {
        s"( select * from table(${schemaPrepender} + ${config.getOrElse("tableFunction", config("sourceTableFunction"))}) ) subTbl"
      } else ""

      //Table Read
      val dbtable =
      if (config.getOrElse("dbtable", config.getOrElse("table", config.getOrElse("sourceTable", ""))).trim.nonEmpty) {
         schemaPrepender + config.getOrElse("dbtable", config.getOrElse("table", config.getOrElse("sourceTable", ""))).trim
      } else ""
      //SQL File Read
      val sqlFile =
        if (config.getOrElse("sqlFile", "").trim.nonEmpty) {
          (getSQLFromTemplate(config).trim + ";")
            .replaceAll(";+$", "")
        } else ""

      if (dbtable.isEmpty && sqlFile.isEmpty && tblFunction.isEmpty ) {
        throw new InvalidArgumentsPassed("must pass sqlFile, tableFunction, dbtable or sourceTable parameter to read the data from table")
      }
      if (sqlFile.nonEmpty) {
        logger.info(s"sql file provided and loaded the config with ${sqlFile}")
        sfConf.put("query", sqlFile)
      } else if (tblFunction.nonEmpty) {
        logger.info(s"table function provided and loaded the config with ${tblFunction}")
        sfConf.put("query", tblFunction)
      } else {
        logger.info(s"loading dataframe Using table(${dbtable}) for selection !!")
        sfConf.put("dbtable", dbtable)
      }

      sfConf.put("sfSchema",
        config.getOrElse("schema",
          config.getOrElse("sourceSchema",
            config.getOrElse("sfSchema",
              config.getOrElse("sourceSfSchema","")
            )
          )
        ).trim
      )
      sfConf.put("sfDatabase",
        config.getOrElse("database",
          config.getOrElse("sourceDatabase",
            config.getOrElse("sfDatabase",
              config.getOrElse("sourceSfDatabase","")
            )
          )
        ).trim
      )

      sfConf.put("sfWarehouse",
        config.getOrElse("warehouse",
          config.getOrElse("sourceWarehouse",
            config.getOrElse("sfWarehouse",
              config.getOrElse("sourceSfWarehouse","")
            )
          )
        ).trim
      )

      sfConf = sfConf ++ config.filter(itm => itm._1.matches("^sf.*"))
      val srcDF =
        sparkSession
          .read
          .format("net.snowflake.spark.snowflake")
          .options(sfConf.toMap)
          .load()
      if (config.getOrElse("filterClause", "").nonEmpty) {
        srcDF.filter(config("filterClause"))
      } else {
        srcDF
      }
    } catch {
      case e: Exception => {
        logger.error("Unable to read data from source Snowflake table")
        throw e
      }
    }
  }
}
