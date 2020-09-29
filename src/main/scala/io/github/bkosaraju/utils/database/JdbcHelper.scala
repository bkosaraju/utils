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

import java.sql.Connection

import io.github.bkosaraju.utils.common.{LoadResourceAsString, Session}
import io.github.bkosaraju.utils.common.{LoadResourceAsString, Session}
import org.springframework.jdbc.core.JdbcTemplate

import scala.language.reflectiveCalls

trait JdbcHelper extends Session with TemplateWrapper {

  implicit class StaticSQLExecutor(val jdbcTemplate: JdbcTemplate) {
    def staticSqlExecute(sqlFile: String): Unit = {
      try {
        jdbcTemplate.update(sqlFile)
      } catch {
        case e: Exception => {
          logger.error("Unable to Execute sql : " + sqlFile, e)
        }
      }
    }
  }

  def using[T <: { def close() }]
  (resource: T)
  (block: T => Unit)
  {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }

  def getDBProductName(jt : Connection): String = {
    var dbType : String = ""
    using(jt){ conn => dbType = conn.getMetaData.getDatabaseProductName }
    dbType
  }



  implicit class GetResourcePath (val jdbcTemplate: JdbcTemplate) extends LoadResourceAsString {
    def getresource( resourceLocation : String ) : String = {
       loadResource("db/" + getDBProductName(jdbcTemplate.getDataSource.getConnection())+"/" + resourceLocation)
    }
  }

  implicit class GetResourcePathForsscalaJdbc (val jdbcTemplate: TemplateWrapper) extends LoadResourceAsString {
    def getresource( resourceLocation : String ) : String = {
      loadResource("db/" + getDBProductName(jdbcTemplate.getDataSource.getConnection)+"/" + resourceLocation)
    }
  }

}
