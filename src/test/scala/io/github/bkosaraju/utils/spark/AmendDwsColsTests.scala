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
import org.apache.spark.sql.types._

trait AmendDwsColsTests extends AppInterface with ContextInterface {


  private val dfSchema = StructType(Seq(
    StructField("src_timestamp", TimestampType , true),
    StructField("src_date", DateType, true),
    StructField("Keycol", IntegerType, true),
    StructField("ValueCol", StringType, true))
  )

  private val srcSchema = StructType(Seq(
    StructField("src_timestamp", StringType , true),
    StructField("src_date", StringType, true),
    StructField("Keycol", StringType, true),
    StructField("ValueCol", StringType, true))
  )

  //private val srcDf = af.loadStdDF("src/test/resources/rand_data.csv","csv",Map("header" -> "true"),srcSchema)
  private val srcDf = context.read.options(Map("header" -> "true")).schema(srcSchema).csv("src/test/resources/rand_data.csv")
  private val tgtSchema = dfSchema
    .add("audit_date",DateType,false)
    .add("audit_id",IntegerType,false)

  test("amendDwsCols : Add Audit columns to source dataframe - Function check") {
    assertResult(500) {
      sparkFunctions.amendDwsCols(srcDf, Map("audit_date" -> "2018-10-05", "audit_id" -> "12345")).count()
    }
  }

  test("amendDwsCols : Add Audit columns to source dataframe - Function check with empty keys") {
    assertResult(500) {
      sparkFunctions.amendDwsCols(srcDf, Map()).count()
    }
  }

  test("amendDwsCols : Add Audit columns to source dataframe - Column count check") {
    assertResult(6) {
      sparkFunctions.amendDwsCols(srcDf,Map("audit_date"->"2018-10-05","audit_id"->"12345")).columns.length
    }
  }

  test("amendDwsCols : Add Audit columns to source dataframe - Return value check") {
    assertResult("2018-10-05#12345") {
      sparkFunctions.amendDwsCols(srcDf,Map("audit_date"->"2018-10-05","audit_id"->"12345"))
        .select("audit_date","audit_id")
        .dropDuplicates.collect.map(x => x.mkString("#")).mkString("")
    }
  }

  test("amendDwsCols : Raise an exception in-case if there is any issue while amending the data warehousing columns") {
    intercept[Exception] {
      sparkFunctions.amendDwsCols(null,Map("audit_date"->"2018-10-05","audit_id"->"12345")).columns.length
    }
  }


}
