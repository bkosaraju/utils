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

package io.github.bkosaraju.utils

import io.github.bkosaraju.utils.common.{MaskSensitiveValuesFromMapTests, SQLTemplateWrapperTests}
import io.github.bkosaraju.utils.database.{DataEncryptorTests, SendMailTests, StringToMapTests, WriteDataTests, cryptoSuiteTests}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spark._

@RunWith(classOf[JUnitRunner])
class Tests
  extends
    DataHashGeneratorTests
    with HubLoaderTests
    with StringToMapTests
    with cryptoSuiteTests
    with DataEncryptorTests
    with WriteDataTests
    with SendMailTests
//Spark Utils
    with AmendDwsColsTests
    with ContextcreationTests
    with ConvNonStdDateTimesTests
    with GetDeltaDFTests
    with SrcToTgtColRenameTests
//Common Utils
    with SQLTemplateWrapperTests
    with MaskSensitiveValuesFromMapTests
