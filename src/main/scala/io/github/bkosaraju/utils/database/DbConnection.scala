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

import io.github.bkosaraju.utils.common.{Exceptions, Session}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.github.bkosaraju.utils.common.{Exceptions, Session}
import org.springframework.jdbc.core.JdbcTemplate
trait DbConnection extends Session with Exceptions {

  def getConnection(params: Map[String,String]): JdbcTemplate = {
    val jdbcURL =
      try {
        if (params.contains("jdbcURL")) {
          params("jdbcURL")
        }
        else {
          if (params.getOrElse("jdbcType", "").toLowerCase.contains("postgres")) {
            s"jdbc:" + params.getOrElse("jdbcType","postgresql") + "://" + params.getOrElse("jdbcHostname", "localhost") +
              ":" + params.getOrElse("jdbcPort", "5432") + "/" + params.getOrElse("jdbcDatabase", "test") +
              "?user=" + params.getOrElse("jdbcUsername", "xxxxxxxx") + "&password=" + params.getOrElse("jdbcPassword", "xxxxxxxx") + "&stringtype=unspecified&sslmode=prefer"
          } else {
            s"jdbc:" + params.getOrElse("jdbcType", "mysql") + "://" + params.getOrElse("jdbcHostname", "localhost") +
              ":" + params.getOrElse("jdbcPort", "3306") + "/" + params.getOrElse("jdbcDatabase", "test") +
              "?user=" + params.getOrElse("jdbcUsername", "meta_user") + "&password=" + params.getOrElse("jdbcPassword", "xxxxxxxxxx")
          }
        }
      }
      catch {
        case e: Exception =>
          logger.error("Unable to Retrieve JDBC parameters" +
            "(the values for either of  jdbcType,jdbcHostname,jdbcPort,jdbcDatabase,jdbcUsername,jdbcPassword not defined) " +
            "please pass the properties file as first argument", e)
          sys.exit(1)
      }


    val driverClass =
      try {
        if (params.contains("driverClass")) {
          params("driverClass")
        }
        else {
          if (params.getOrElse("jdbcType", "").toLowerCase.contains("postgres")) {
            params.getOrElse("jdbcDriverClass", "org.postgresql.ds.PGPoolingDataSource")
          } else {
            params.getOrElse("jdbcDriverClass", "org.mariadb.jdbc.MariaDbDataSource")
          }
        }
      } catch {
        case e: Exception =>
          logger.error("JDBC Driver Class is not defined please set the variable jdbcDriverClass", e)
          throw UnknownDriverClass("JDBC Driver Class is not defined please set the variable jdbcDriverClass")
      }

    var config = new HikariConfig()

    config.addDataSourceProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource")
    config.setJdbcUrl(jdbcURL)


    config.setLeakDetectionThreshold(params.getOrElse("leakDetectionThreshold",2000.toLong).toString.toLong)
    config.setMaximumPoolSize(params.getOrElse("poolSize",2).toString.toInt)
    config.setIdleTimeout(params.getOrElse("idleTimeout",10000).toString.toLong)
    config.setConnectionTimeout(params.getOrElse("connectionTimeout",30000).toString.toLong)
    config.setMaxLifetime(params.getOrElse("maxLifeTime",1800000).toString.toLong)

    val conn = new HikariDataSource(config)
    new JdbcTemplate(conn)
  }
}