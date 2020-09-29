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

import java.text.SimpleDateFormat
import java.util.TimeZone

trait GetDate extends Exceptions {

  def getDate(timezone: String = ""): java.text.SimpleDateFormat = {
    if ("".equals(timezone)) {
      val tzUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      tzUTC.setTimeZone(TimeZone.getTimeZone("GMT"))
      tzUTC
    } else {
      val tmz = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      tmz.setTimeZone(TimeZone.getTimeZone(timezone))
      tmz
    }
  }

  def getTimestamp( datetime : String ) : String = {
    val tsFormat = """(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z{0,1}""".r
    val dtFormat = """(\d{4}-\d{2}-\d{2})""".r
    datetime match {
      case tsFormat(ts) => ts.replaceAll("T"," ")
      case dtFormat(dt) => dt+" 00:00:00"
      case _ => throw UnknownDateFormatException("date should be passed only in YYYY-mm-ddTHH:MM:SS[Z] or YYYY-mm-DD")
    }
  }
}
