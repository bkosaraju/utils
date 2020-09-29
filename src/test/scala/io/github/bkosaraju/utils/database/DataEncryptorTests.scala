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

package io.github.bkosaraju.utils.database

import io.github.bkosaraju.utils.AppInterface
import io.github.bkosaraju.utils.spark.DataEncryptor._

trait DataEncryptorTests extends AppInterface {


  private val t1SQL = """select '2018/12/14 12:10:21' as src_timestamp, '15/04/2018' as src_date"""
  test("DataEncryptorTests: Able to encrypt given input columns in dataframe") {
    val sDF = context.sql(t1SQL)
    assertResult("yrKB+/crIgTVjv9xLp4njFFBq8YNaOA4fxvxp+cKg2o=") {
      val hDF = sDF.encrypt("someKey", "src_timestamp", "src_date")
      hDF.show(false)
      hDF.select("src_timestamp").collect.map(_.getString(0)).head
    }
  }

  test("DataEncryptorTests: Able to decrypt given input columns in dataframe") {
    val sDF = context.sql(t1SQL)
    assertResult("15/04/2018") {
      val hDF = sDF.encrypt("randomKey", "src_date")
      println("Encrypted Dataset:")
      hDF.show(false)
      val dDF = hDF.decrypt("randomKey", "src_date")
      println("Decrypted Dataset:")
      dDF.show(false)
      dDF.select("src_date").collect.map(_.getString(0)).head
    }
  }

  private val t2SQL = """select '2018/12/14 12:10:21' as src_timestamp, cast('2019-09-06' as date) as src_date"""
  test("DataEncryptorTests: Able to decrypt given input columns in dataframe(Date DataType)") {
    val sDF = context.sql(t2SQL)
    assertResult("2019-09-06") {
      val hDF = sDF.encrypt("randomKey", "src_date")
      println("Encrypted Dataset:")
      hDF.show(false)
      val dDF = hDF.decrypt("randomKey", "src_date")
      println("Decrypted Dataset:")
      dDF.show(false)
      dDF.select("src_date").collect.map(_.getString(0)).head
    }
  }

  test("DataEncryptorTests: Throw exception in case if not be able te encrypt columns") {
    val sDF = context.sql(t2SQL)
    intercept[Exception] {
      val hDF = sDF.encrypt("randomKey", "unKnownColumn")
      hDF.show(false)
    }
  }

  test("DataEncryptorTests: Throw exception in case if not be able te decrypt columns") {
    val sDF = context.sql(t2SQL)
    intercept[Exception] {
      val hDF = sDF.decrypt("randomKey", "aaaaa")
      hDF.show(false)
    }
  }

  test("DataEncryptorTests: Throw exception in case if not be able te decrypt columns(with out encryption)") {
    val sDF = context.sql(t2SQL)
    intercept[Exception] {
      val hDF = sDF.decrypt("randomKey", "src_date")
      hDF.show(false)
    }
  }

}