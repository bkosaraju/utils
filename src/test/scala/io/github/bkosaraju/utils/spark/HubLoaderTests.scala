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

import io.github.bkosaraju.utils.AppInterface
import org.apache.spark.sql.functions.{col, lit}


trait HubLoaderTests extends AppInterface {
  import context.implicits._
  import sparkFunctions._
  private val df = (1 to 10).toDF("col1").withColumn("col2",col("col1"))
  private val df2 = df.filter(col("col1").notEqual(lit(3)))
  test("HubLoader: Able to generate hash for given input dataframe") {
    val sourceData = df.genHash(Seq("col1","col2"))
    val targetData = df2.genHash(Seq("col1","col2"))
    assertResult(3) {
      val hDF = sourceData.getChanges(targetData)
      hDF.show(false)
      hDF.select("col1").collect.map(_.getInt(0)).head
    }
  }

  test("HubLoader: Able to generate hash for given input dataframe compared with given Key column") {
    val sourceData = df.genHash(Seq("col1","col2"))
    val targetData = df2.genHash(Seq("col1","col2"))
    assertResult(3) {
      val hDF = sourceData.getChanges(targetData,Option(Seq("col1")))
      hDF.show(false)
      hDF.select("col1").collect.map(_.getInt(0)).head
    }
  }

  test("HubLoader: Throw exception incase if source code is unable to produce delta changes") {
    val sourceData = df.genHash(Seq("col1","col2"))
    val targetData = df2.genHash(Seq("col1","col2"),"newHash")
    intercept[Exception] {
      val hDF = sourceData.getChanges(targetData)
      hDF.show(false)
      hDF.select("col1").collect.map(_.getInt(0)).head
    }
  }
}
