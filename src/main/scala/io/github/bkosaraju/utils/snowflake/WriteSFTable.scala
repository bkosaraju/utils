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
import io.github.bkosaraju.utils.spark.{Context, SchemaComparison, WriteData}
import io.github.bkosaraju.utils.spark.{Context, SchemaComparison, WriteData}
import org.apache.spark.sql.DataFrame

trait WriteSFTable
  extends Context
    with SchemaComparison
    with WriteData
    with Common {

  def writeSFTable(config: Map[String, String], tgtDF: DataFrame): Unit = {

    var tOps: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]() ++ config.getOrElse("writerOptions", "").stringToMap ++ config

    val inputSchema = config.getOrElse("schema",
      config.getOrElse("schema",
        config.getOrElse("targetSchema",
          config.getOrElse("sfSchema",
            config.getOrElse("targetSfSchema", "UN_KNOWN_SCHEMA")
          )
        )
      )
    ).trim

    tOps.put("sfDatabase",
      config.getOrElse("database",
        config.getOrElse("targetDatabase",
          config.getOrElse("sfDatabase",
            config.getOrElse("targetSfDatabase", "UN_KNOWN_DB"
            )
          )
        )
      ).trim
    )

    tOps.put("sfWarehouse",
      config.getOrElse("warehouse",
        config.getOrElse("targetWarehouse",
          config.getOrElse("sfWarehouse",
            config.getOrElse("targetSfWarehouse", "UN_KNOWN_WH")
          )
        )
      ).trim
    )

    val schemaPrepender = if (inputSchema.nonEmpty) {
      logger.info(s"loading data from schema ${inputSchema}")
      inputSchema + "."
    } else {
      logger.warn("Unable to identify InputSchema deriving from other parameters")
      ""
    }

    val dbtable = schemaPrepender + config.getOrElse("table", config("targetTable"))
    tOps.put("writerFormat", "net.snowflake.spark.snowflake")
    tOps.put("dbtable", dbtable)
    tOps.put("sfSchema", inputSchema)
    tOps.remove("table")
    tOps.remove("warehouse")
    tOps.remove("database")
    tOps.remove("schema")

    val tgtSchema = sparkSession
      .read
      .format("net.snowflake.spark.snowflake")
      .options(tOps)
      .load()

    val schemaValidator = ((tgtDF.schema.diff(tgtSchema.schema).map(_.name.toLowerCase()) == List(config.getOrElse("hashedColumn", "row_hash").toLowerCase)) ||
      (isSchemaSame(tgtSchema.schema, tgtDF.schema)) ||
      (!isSchemaSame(tgtSchema.schema, tgtDF.schema) && config.getOrElse("inferTargetSchema", "false").equalsIgnoreCase("true")))

    if (!schemaValidator) {
      throw new Exception(s"Source and target Schemas are not matching, Source Schema : ${tgtDF.schema}  -- Target Schema ${tgtSchema.schema}")
    }

    val sfWriteDF = tgtDF
      .select(tgtSchema.columns.head, tgtSchema.columns.tail: _*) //Hack to fix column order issue posed by Snowflake writer
    writeDataFrame(sfWriteDF, dbtable, config.getOrElse("writeMode", "append"), tOps.toMap)
  }

}
