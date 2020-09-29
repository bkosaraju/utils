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

import io.github.bkosaraju.utils.common.Session
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SizeEstimator

trait WriteData
  extends Session
{

    def getNumParts (Df:DataFrame,
                     blockSize: Long = 1342177280 ): DataFrame = {
      Df.repartition(
      ( SizeEstimator.estimate(Df) / blockSize ).toInt + 1
      )
    }
  /**
   * Writes the data Into HDFS location
   * this is the highlevel API which will write to target specific data formats.
   * this has some of the lowlevel API call which will be described in other sections.
   * @param Df dataframe to be wirtten
   * @param targetPath target Path where the data should be written
   * @param writeMode write mode - append or overwrite
   * @param writeFormat target write format such as jdbc/rdbms, snowflake, parquet, orc , avro etc etc..
   * @param writerOptions target writer options as Map of key value pairs
   * @param partitionColumns any of target partition columns
   */
  def writeDataFrame(Df: DataFrame,
                     targetPath: String,
                     writeFormat: String,
                     writeMode : String,
                     writerOptions : Map[String,String],
                     partitionColumns : String = ""
                    ): Unit = {
    try {
      val targetDF = getNumParts(Df, writerOptions.getOrElse("blockSize", 1342177280).toString.toLong)

      if (partitionColumns.trim.isEmpty) {
        targetDF.write.options(writerOptions).mode(writeMode).format(writeFormat).save(targetPath)
      } else {
        targetDF.write.partitionBy(partitionColumns).options(writerOptions).mode(writeMode).format(writeFormat).save(targetPath)
      }
      logger.info("Successfully loaded the path "+targetPath)
    } catch {
      case e: Throwable =>
        logger.error("Unable to Load Data Into Path : " + targetPath + "\n", e)
        throw e
    }
  }

  def writeDataFrame(
                      Df: DataFrame,
                      dbtable: String,
                      writeMode : String,
                      writerOptions : Map[String,String]
                    ): Unit = {

    writeDataFrame(Df,
      writeMode,
      writerOptions ++ Map("dbtable" -> dbtable)
    )
  }


  def writeDataFrame(
                      Df: DataFrame,
                      writeMode : String,
                      writerOptions : Map[String,String]  ): Unit = {
    try {
      if (writerOptions.keySet.map(_.toLowerCase).contains("dbtable") || writerOptions.keySet.contains("location")) {
        if (writerOptions.keySet.map(_.toLowerCase).contains("dbtable") && writerOptions.contains("url")) {
          //writing JDBC Data
          Df.write.options(writerOptions).mode(writeMode).format("jdbc").save()
          logger.info("Successfully loaded table " + writerOptions("dbtable"))
        } else if (writerOptions.keySet.map(_.toLowerCase).contains("dbtable") && writerOptions.getOrElse("writerFormat","").nonEmpty) {
          //Writing Snowflake type data
          if ( Df.isEmpty ) {
            logger.warn("empty input generated hence skip target load ...")
          } else {
            Df.write.options(writerOptions).mode(writeMode).format(writerOptions("writerFormat")).save()
            logger.info("Successfully loaded table " + writerOptions("dbtable"))
          }
        } else if (writerOptions.keySet.map(_.toLowerCase).contains("dbtable")) {
          //Writing Hive Data
          Df.write.options(writerOptions).mode(writeMode).insertInto(writerOptions("dbtable"))
          logger.info("Successfully loaded table " + writerOptions("dbtable"))
        } else {
          //Writing File Data into Location
          getNumParts(Df, writerOptions.getOrElse("blockSize", 1342177280).toString.toLong)
            .write.options(writerOptions).mode(writeMode).save(writerOptions.getOrElse("location", ""))
        }
      } else {
        throw new IllegalArgumentException("dataframe writer needs either dbtable or location argument")
      }
    } catch{
      case e: Throwable =>
        logger.error("Unable to Load Data Into target : " +  writerOptions.getOrElse("dbtable",writerOptions.getOrElse("location","")))
        throw e
    }
  }
}
