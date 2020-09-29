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
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp,lit,when,not,trim}
import org.apache.spark.sql.types.StructType

trait ConvNonStdDateTimes extends Context {
  /** Convert the Non standard DataTypes into standard timestamps in source Dataframe with the given input map of input datatype pattern
   * at the same time it converts the same dataframe into target data type
   *
   * @example convNonStdDateTimes(srcDF '''DataFrame''',soruceDatePattern '''Map of Column name to Column format''', tgtSchema '''Schema''' )
   * @param srcData        source dataframe where the columens need to be updated
   * @param dtTmCols  Key Value pair of the columns to be paaded to the dataframe
   * @return DataFrame of Format converted Dates & Timestamp
   *         {{{
   *          sDF
   *                  .withColumn(clmn,
   *                    from_unixtime(
   *                      unix_timestamp(
   *                        col(clmn),
   *                        dtTmCols(clmn))
   *                    ))
   *                  )
   *         }}}
   */

  def convNonStdDateTimes(srcData: DataFrame, dtTmCols: Map[String, String]): DataFrame = {
    try {
      // val srcData = df.withColumn("dqvalidityFlag", lit("Y"))
      if (dtTmCols.nonEmpty) {

        dtTmCols.keys.toList.foldLeft(srcData)((sDF, clmn) => {
          sDF
            .withColumn(clmn,
              from_unixtime(
                unix_timestamp(
                  trim(col(clmn)),
                  dtTmCols(clmn))
              )
            )})
      } else srcData
    } catch {
      case e: Exception => logger.error("Unable to Convert Non-Standard Data Types", e)
        throw e
    }
  }
}