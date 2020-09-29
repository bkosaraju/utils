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

trait GetDeltaDF extends Context {
  def getDeltaDF(srcDF: DataFrame, targetDF: DataFrame, config: Map[String, String]): DataFrame = {
    try {
      srcDF.join(
        targetDF,
        Seq(config.getOrElse("hashedColumn", "row_hash")),
        "left_anti")
    } catch {
      case e: Exception => {
        logger.error("Unable to compute Delta for given data frames")
        throw e
      }
    }
  }
}