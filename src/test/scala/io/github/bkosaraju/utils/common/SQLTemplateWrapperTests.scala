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

package io.github.bkosaraju.utils.common

import io.github.bkosaraju.utils.AppInterface
import io.github.bkosaraju.utils.AppInterface

trait SQLTemplateWrapperTests extends AppInterface {

  test ("templateWrapper: Raise an exception in case if there is any issue with template building") {
    intercept[Exception] {
      commonUtils.getSQLFromTemplate(Map[String,String]())
    }
  }

  test ("templateWrapper: Successfully transform the template") {
    assertResult(true) {
      var tp = collection.mutable.Map[String,String]()
      tp.put("k1","v1")
      tp.put("k2","v2")
      tp.put("sqlFile","src/test/resources/templateWrapper/sample.sql")
      commonUtils.getSQLFromTemplate(tp.toMap).contains("k1=v1").equals(commonUtils.getSQLFromTemplate(tp.toMap).contains("k2=v2")).equals(true)
    }
  }

}