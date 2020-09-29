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

trait WriteToTable extends Session{

  def writeToTable(
                      Df: DataFrame,
                      writeFormat: String,
                      writeMode : String,
                      writerOptions : Map[String,String]
                    ): Unit = {

    try {
      Df.write.options(writerOptions).mode(writeMode).format(writeFormat).save()
      logger.info("Successfully loaded table "+writerOptions.getOrElse("dbtable",""))
    } catch {
      case e: Throwable =>
        logger.error("Unable to Load Data Into targetTable : " +  writerOptions.getOrElse("dbtbl","NOT_PROVIDED"))
        throw e
    }
  }

  def writeToTable(
                    Df: DataFrame,
                    writeFormat: String,
                    writeMode : String,
                    dbtable : String,
                    writerOptions : Map[String,String]
                  ): Unit = {
      writeToTable(Df,
        writeFormat,
        writeMode,
        writerOptions ++ Map("dbtable" -> dbtable)
      )
  }


  def writeToTable(
                    Df: DataFrame,
                    tragetDatabase: String,
                    targetTable: String,
                    writeFormat: String,
                    writeMode : String,
                    writerOptions : Map[String,String]
                  ): Unit = {
    writeToTable(Df,
      writeFormat,
      writeMode,
      writerOptions ++ Map("dbtable" -> (tragetDatabase + "." + targetTable))
    )
  }

}
