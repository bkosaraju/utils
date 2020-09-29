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
import io.github.bkosaraju.utils.common.Session
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, sha2}

trait DataHashGenerator {

  /**
   * == DataHashGenerator Implicit class which extends base dataframe class ==
   * @param df - current dataframe (data that being processed now)
   * BeanPropertes
   *  - shaBitLength  -  one of 224, 256, 384, or 512.
   *  - agmntdColName - padded column Name defaulted to hash
   *  - columnSeparator - concatenated column Separator, defaulted to Unit Separator(\u0001F)
   *
   */
  implicit class DataHashGenerator(var df: DataFrame) extends Session {
    /**
     * generate hash of the given column sequence and pad to same data frame as column "hash"(can be updated via setAugmentedColumnName method)
     * @param colSeq input Column list in sequence
     * @return Dataframe with hashpadded column
     * @note if there are any nulls in given column list that shall be excluded and produce the sha without them.
     */
    implicit def genHash(colSeq: Seq[String],
                         agmntdColName : String = "hash",
                         shaBitLength: Int = 256,
                         columnSeparator: String = "\u001F" ): DataFrame = {
      try {
        df.withColumn(
          agmntdColName,
          sha2(
              concat_ws(
              columnSeparator,
              colSeq.map(x => col(x)): _*),
            shaBitLength
          )
        )
      } catch {
        case e: Exception => {
          logger.error("Unable to generate DataHash..", e)
          throw e
        }
      }
    }
  }
}
