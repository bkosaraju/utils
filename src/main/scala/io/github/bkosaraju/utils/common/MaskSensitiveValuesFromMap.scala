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

class MaskSensitiveValuesFromMap {

  var sensitiveKeys :Seq[String] = _
  var configMap : Map[String,String] = _

  def maskSensitiveValuesFromMap : Map[String,String] = {
    collection.mutable.LinkedHashMap(configMap.toSeq.sortBy(_._1):_*)
      .map(x =>
        if (sensitiveKeys.exists(k => x._1.toLowerCase.contains(k))) {
          x._1 ->"**********"
        } else  x ).toMap
  }
  def setSensitiveKeys(sensitiveKeys: Seq[String]) : Unit = {
    this.sensitiveKeys = sensitiveKeys
  }
  def setConfigMap (config: Map[String,String] ) : Unit = {
    this.configMap = config
  }
}

object MaskSensitiveValuesFromMap {
  def apply(config:  Map[String,String], sensitiveKeys : Seq[String] = Seq("password","secret","token","encryptionkey","privatekey","publickey")): Map[String,String] = {
    val ms = (new MaskSensitiveValuesFromMap())
    ms.setConfigMap(config)
    ms.setSensitiveKeys(sensitiveKeys)
    ms.maskSensitiveValuesFromMap
  }
}
