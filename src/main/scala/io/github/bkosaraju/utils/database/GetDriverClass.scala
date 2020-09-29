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

import java.util.InputMismatchException

import io.github.bkosaraju.utils.spark.Context

trait GetDriverClass extends Context {
  /**
   * Method to generate the pre loaded library list for Driver class names, however this method will be over written when user explicitly
   * specifies the Driver namae from readerOptios=Driver=com.some.Driver
   *
   * @param url : Driver URL where the RDMS type will be extracted
   * @return Driver Name for Application
   * @example getDriverClass(jdbcURL)
   *          {{{
   *            url match {
   *                 case url if url.toLowerCase.contains("teradata") => "com.teradata.jdbc.TeraDriver"
   *                 case url if url.toLowerCase.contains("mysql") => "com.mysql.jdbc.Driver"
   *                 case url if url.toLowerCase.contains("postgres") => "org.postgresql.Driver"
   *                 case url if url.toLowerCase.contains("mariadb") => "org.mariadb.jdbc.Driver"
   *                 case url if url.toLowerCase.contains("oracle") => "oracle.jdbc.driver.OracleDriver"
   *                 case url if url.toLowerCase.contains("hive") => "org.apache.hadoop.hive.jdbc.HiveDriver"
   *                 case url if url.toLowerCase.contains("h2") => "org.h2.Driver"
   *                 case url if url.toLowerCase.contains("sqlserver") => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   *                 case _ => throw new InputMismatchException
   *          }}}
   */
  def getDriverClass (url : String ) : String = {
    try {
      url match {
        case url if url.toLowerCase.contains("teradata") => "com.teradata.jdbc.TeraDriver"
        case url if url.toLowerCase.contains("mysql") => "com.mysql.jdbc.Driver"
        case url if url.toLowerCase.contains("postgres") => "org.postgresql.Driver"
        case url if url.toLowerCase.contains("mariadb") => "org.mariadb.jdbc.Driver"
        case url if url.toLowerCase.contains("oracle") => "oracle.jdbc.driver.OracleDriver"
        case url if url.toLowerCase.contains("hive") => "com.simba.hive.jdbc41.HS2Driver"
        case url if url.toLowerCase.contains("h2") => "org.h2.Driver"
        case url if url.toLowerCase.contains("sqlserver") => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        case _ => throw new InputMismatchException("Input URL is not matching any of known driver classes")
      }
    } catch {
      case e : Exception => logger.error("Unable to Extract the Driver URL for given Driver ",e)
        throw e
    }
  }
}