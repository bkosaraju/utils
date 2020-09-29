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

trait StringToMap extends Session {
  /**
   * Parameter conversion function used to derive the source strings into Maps
   * this is predominantly used across source to target column renames, source datatypes of timestamp and date fields in along the ammending the datawarehousing fileds.
   *
   * @param extParams input params of type k1=v1,k2=v2
   * @return Map of (String,String)
   * {{{param.split(",").map(x => {
   *                             val k = x.split("="); (k(0), k(1))
   *                           }).toList.toMap}}}
   */
  implicit class StringToMap(extParams: String) {
    /**
     * Parameter conversion function used to derive the source strings into Maps
     * this is predominantly used across source to target column renames, source datatypes of timestamp and date fields in along the ammending the datawarehousing fileds.
     *
     * @return Map of (String,String)
     * {{{param.split(",").map(x => {
     *                             val k = x.split("="); (k(0), k(1))
     *                           }).toList.toMap}}}
     */
    def stringToMap: Map[String, String] = {
      val param = extParams.replaceAll("^\"|\"$", "")
      try {
        if (param.length != 0) {
          //Negative look back to handle the separator is used as value itself
          param.split("(?<!(=)),").map(x => {
            val k = x.split("(?<!(=))=");
            (k(0), k(1))
          }).toList.toMap
        } else Map[String, String]()
      } catch {
        case e: Exception =>
          logger.error("Unable to Convert String to Map : " + extParams + " to Map", e)
          throw e
      }
    }
  }

}
