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
import io.github.bkosaraju.utils.snowflake.WriteSFTable
import io.github.bkosaraju.utils.spark.DataEncryptor._

trait DataFrameWriter
  extends StringToMap
    with WriteData
    with ConvNonStdDateTimes
    with SrcToTgtColRename
    with AmendDwsCols
    with SchemaComparison
    with WriteSFTable
    {
  def dataframeWriter(srcConfig: Map[String, String], srcDF: org.apache.spark.sql.DataFrame): Unit = {
    try {
      var config = srcConfig
      if (srcConfig.getOrElse("writeMode","append").equalsIgnoreCase("delta")) {
        config = srcConfig + ("writeMode" -> "append")
      }
      val targetKey = config.getOrElse("targetKey","")
      val srcFormats = config.getOrElse("srcFormats", "").replaceAll("\"", "").stringToMap
      val srctoTgtMap = config.getOrElse("srcToTgtColMap", "").stringToMap

      val confDF = convNonStdDateTimes(
        srcToTgtColRename(
          amendDwsCols(srcDF, config.getOrElse("dwsVars", "").stringToMap)
          , srctoTgtMap)
        , srcFormats
      )
      val tgtDF =
        if (config.contains("encryptionKey") && config.contains("encryptionColumns")) {
          logger.info("proceeding to perform column level encryption ..")
          (new DataEncryptor(confDF)).encrypt(config("encryptionKey"), config("encryptionColumns").split(","): _*)
        } else {
          logger.info("encryptionKey/encryptionColumns[both must] not provided hence skipping data column encryption..")
          confDF
        }

      if (Seq("jdbc","rdbms").contains(config.getOrElse("writerFormat","").toLowerCase)) {
        val dbtable = config.getOrElse("database",config.getOrElse("targetDatabase","")) + "." + config.getOrElse("table",config.getOrElse("targetTable",""))
        val tgtSchema = sparkSession
          .read
          .format("jdbc")
          .options(config)
          .option("dbtable",dbtable)
          .load()

        val targetData =
        if ( (tgtDF.schema.diff(tgtSchema.schema).map(_.name.toLowerCase()) == List(config.getOrElse("hashedColumn", "row_hash").toLowerCase))) {
          tgtDF.drop(config.getOrElse("hashedColumn", "row_hash"))
        } else if ( config.getOrElse("inferTargetSchema","false").equalsIgnoreCase("true")) {
          tgtDF.select(tgtSchema.columns.head, tgtSchema.columns.tail : _*)
        } else {
          tgtDF
        }
        writeDataFrame(
          targetData,
          dbtable,
          config.getOrElse("writeMode", "append"),
          config.getOrElse("writerOptions", "").stringToMap)
      } else if (Seq("snowflake","net.snowflake.spark.snowflake").contains(config.getOrElse("writerFormat","").toLowerCase()))  {
        writeSFTable(config, tgtDF)
      } else {
        writeDataFrame(
          tgtDF,
          config.getOrElse("targetURI", "")
            .replaceAll("[sS]3://","s3a://")
            .replace("/$", "") + "/" + targetKey,
          config.getOrElse("writerFormat", "orc"),
          config.getOrElse("writeMode", "append"),
          config.getOrElse("writerOptions", "").stringToMap
        )
      }
    } catch {
      case e: Exception => {
        logger.error("Unable to process dataframe and write to target area..")
        throw e
      }
    }
  }
}

