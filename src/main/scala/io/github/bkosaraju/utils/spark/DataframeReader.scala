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

import io.github.bkosaraju.utils.database.GetDriverClass
import io.github.bkosaraju.utils.snowflake.ReadSFTable
import org.apache.spark.sql.DataFrame

trait DataframeReader
  extends ReadSFTable
  with GetDriverClass{
  /**
   * loads the schema driven files into a dataframe.
   *
   * @param config any options that should be passed to low level spark api to read the data such as skip the header or any other formats.
   * @return Data from with source data.
   * @example loadStdDF("jdbc", readerOptions)
   * {{{
   *                   sparkSession.read.format(readerFormat).options(readerOptions).load()
   * }}}
   */
  def dataframeReader(config: Map[String,String] ): DataFrame = {
    try {
      val resConf : collection.mutable.Map[String,String] = collection.mutable.Map()
      if (Seq("jdbc", "rdbms").contains(config.getOrElse("readerFormat","").toLowerCase)) {
        resConf.put("url", config.getOrElse("url", config.getOrElse("rdbmsUrl", "NOT_PROVIDED")))
        resConf.put("user", config.getOrElse("user", config.getOrElse("rdbmsUser", "NOT_PROVIDED")))
        resConf.put("password", config.getOrElse("password", config.getOrElse("rdbmsPassword", "NOT_PROVIDED")))
        resConf.put("readerFormat","jdbc")
        if (config.contains("Driver")) {
          resConf.put("Driver", config("Driver"))
        } else {
          resConf.put("Driver", getDriverClass(resConf("url")))
        }
        if (config.getOrElse("sqlFile", "").ne("")) {
          resConf.put("dbtable",
            (
              (getSQLFromTemplate(config).trim + ";")
                .replaceAll("^", " ( ")
                .replaceAll(";+$", " ) sbTbl"))
          )
        } else {
          logger.info("No SQL file provided, continuing with full table extract !!")
          resConf.put("dbtable",
            (config.getOrElse("database",config("sourceDatabase")) + "." + config.getOrElse("table",config("sourceTable")))
          )
        }
      }
      val readerConfig = config ++ resConf

      if (Seq("snowflake","net.snowflake.spark.snowflake").contains(readerConfig.getOrElse("readerFormat","").toLowerCase()))  {
        readSFTable(readerConfig)
      } else if (readerConfig.getOrElse("readerFormat","").nonEmpty) {
        val resDF = sparkSession.read.format(readerConfig("readerFormat")).options(readerConfig).load()
          if (readerConfig.getOrElse("filterClause","").trim.nonEmpty) {
            resDF.filter(readerConfig("filterClause"))
          } else {
            resDF
          }
      } else {
        throw new InvalidArgumentsPassed(s"Unknown Reader config passed ..")
      }
    } catch {
      case e: Throwable =>
        logger.error("Unable to Load Dataframe with given Reader Options", e)
        throw e
    }
  }
}