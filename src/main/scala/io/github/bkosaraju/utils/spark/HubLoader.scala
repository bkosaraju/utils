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
import org.apache.spark.sql.functions.col

trait HubLoader extends Session {
  /**
   * == HubLoader Implicit class which extends base dataframe class and generate delta results==
   * @param df - current dataframe (data that being processed now)
   * BeanPropertes
   *  - augmentedColName - padded column Name defaulted to hash
   */
  implicit class HubLoader(df: DataFrame) {

    //@BeanProperty var augmentedColName = "hash"

    /**
     * extract only the data which has recent changes. that being computed using hash which is generated.
     * @param targetDF - target dataframe which is created using  target table.
     * @param keyColumns - any key columns to be considered while joining these two dataframes (makes more resilient to sha collision)
     * @return Delta dataframe.
     */
    def getChanges(targetDF: DataFrame, keyColumns: Option[Seq[String]] = None , augmentedColName : String ="hash"): DataFrame = {
      try {
        val joinColumns =
          keyColumns match {
            case None => Seq(augmentedColName)
            case _ => keyColumns.get :+ augmentedColName
          }
       val tDF =
        df.as("latest")
          .join(
            targetDF.as("persisted"),
            joinColumns,
            "left_outer"
          )
          .filter(col("persisted." + augmentedColName).isNull)

          tDF.select(df.columns.map(x => df.col(x)): _*)
      } catch {
        case e: Exception => {
          logger.error("Unable to get Delta Dataframe out of give two dataframes")
          throw e
        }
      }
    }
  }
}
