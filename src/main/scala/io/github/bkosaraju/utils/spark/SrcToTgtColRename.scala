

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

import org.apache.spark.sql.DataFrame

trait SrcToTgtColRename extends Context {
  /**
   * Method to rename the columns from source to target
   *
   * @param df
   * @param mappings
   * @return Dataframe with renamed columns
   *         {{{        mappings.keys.foldLeft(df)((cDF, clmn) => cDF.withColumnRenamed(clmn, mappings(clmn))) }}}
   *
   */
  def srcToTgtColRename(df: DataFrame, mappings: Map[String, String]=Map() ): DataFrame = {
    if (mappings.nonEmpty) {
      try {
        mappings.keys.foldLeft(df)((cDF, clmn) => cDF.withColumnRenamed(clmn, mappings(clmn)))
      } catch {
        case e: Exception =>
          logger.error("Unable to Rename the Given Column", e)
          throw e
      }
    } else df
  }
}

