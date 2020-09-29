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
import io.github.bkosaraju.utils.{AppInterface, ContextInterface}
import org.apache.spark.sql.functions._

trait ConvNonStdDateTimesTests extends AppInterface with ContextInterface {
  import commonUtils._

  private val t1SQL = """select '2018/12/14 12:10:21' as src_timestamp, '15/04/2018' as src_date"""
  private val t2SQL = """select cast('2018-12-14 12:10:21' as timestamp) as src_timestamp, cast ('2018-04-15' as date) as src_date"""
  private val srcFormat = "src_timestamp=yyyy/MM/dd HH:mm:ss,src_date=dd/MM/yyyy"

  test("convNonStdDateTimes : Test Non Standard data types being converted to Standard date and time values - check the count") {
    val sDF = context.sql(t1SQL)
    val tDF = context.sql(t2SQL)
    val srcMap = srcFormat.stringToMap 
    val rDF = sparkFunctions.convNonStdDateTimes(sDF, srcMap)
    assertResult(1) {
      tDF.count
    }
  }

  private val sDF = context.sql(t1SQL)
  private val tDF = context.sql(t2SQL)
  private val srcMap = srcFormat.stringToMap
  private val rDF = sparkFunctions.convNonStdDateTimes(sDF, srcMap)


  test("convNonStdDateTimes : Test Non Standard data types being converted to Standard date and time values - check data ") {
    assertResult("2018-04-15") {
      rDF.select(col("src_date").cast("date")).collect.map(x => x(0)).mkString("")
    }
  }
  test("convNonStdDateTimes : Test Non Standard data types being converted to Standard date and time values - check results ") {
    rDF.show()
    assertResult("2018-12-14 12:10:21~2018-04-15") {
      rDF.select(from_unixtime(unix_timestamp(col("src_timestamp")), "yyyy-MM-dd HH:mm:ss")
        ,col("src_date").cast("date")).rdd.collect().map(x => x.mkString("~")).take(1).mkString("")
    }
  }

  test("convNonStdDateTimes : Raise an exception incase if there is any issue with Converting non Standard date times") {
     intercept[Exception] {
       sparkFunctions.convNonStdDateTimes(null, srcMap)
    }
  }

}
