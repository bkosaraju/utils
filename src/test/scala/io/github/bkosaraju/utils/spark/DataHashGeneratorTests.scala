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
import io.github.bkosaraju.utils.AppInterface

trait DataHashGeneratorTests extends AppInterface {
  import sparkFunctions._
  private val t1SQL = """select '2018/12/14 12:10:21' as src_timestamp, '15/04/2018' as src_date"""
  test("DataHashGenerator: Able to generate hash for given input dataframe") {
    val sDF = context.sql(t1SQL)
    assertResult("d37a79a929dcaee8e4ea014137e4b309aa8b3fad96e3da153db0c777c965672a") {
      val hDF = sDF.genHash(Seq("src_timestamp", "src_date"))
      hDF.show(false)
      hDF.select("hash").collect.map(_.getString(0)).head
    }
  }

  test("DataHashGenerator: Throw exception while not be able to generate hash") {
    var sDF = context.sql(t1SQL)
    intercept[Exception] {
      val hDF = sDF.genHash(Seq("src_timestamp", "src_date"),shaBitLength = 500)
      hDF.show(false)
      hDF.select("hash").collect.map(_.getString(0)).head
    }
  }

  test("DataHashGenerator: generate Hash for SHA512") {
    val sDF = context.sql(t1SQL)
    assertResult("e34cefded090241d892b308ee3fb1ac792c25ce6fc4c0360a1eb12455159961267e115608fbabb1abecb83320b1c897ed0f6c3b601fb6e2c74fadc4abfdcad49") {
      val hDF = sDF.genHash(
        colSeq = Seq("src_timestamp", "src_date"),"dataHash",512,"\u001F")
      hDF.show(false)
      hDF.select("dataHash").collect.map(_.getString(0)).head
    }
  }
}
