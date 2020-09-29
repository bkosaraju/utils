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

import io.github.bkosaraju.utils.{AppInterface, ContextInterface}
import io.github.bkosaraju.utils.AppInterface

trait ContextcreationTests extends AppInterface {

  val t1SQL = """select '2018/12/14 12:10:21' as src_timestamp, '15/04/2018' as src_date"""
  test("Context : Context Creation for SQL") {
    val sDF = context.sql(t1SQL)
    assert(sDF.count == 1)
  }
}
