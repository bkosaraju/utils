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

import javax.sql.DataSource
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import org.springframework.lang.Nullable

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

trait TemplateWrapper {

  /**
   * Helper function to Convert JDBC Template to Scala Compatible one
   *
   * @param jt java JDBC Template
   */

  implicit class TemplateWrapper(jt: JdbcTemplate) {

    @throws(classOf[DataAccessException])
    def queryForObject[T: ClassTag](sql: String, args: Any*): Option[T] = {
      Option(jt.queryForObject(sql,
        asInstanceOfAnyRef(args).toArray,
        typeToClass[T]))
    }

    @throws(classOf[DataAccessException])
    def queryForSeq[T: ClassTag](sql: String): Seq[T] = {
      jt.queryForList(sql, typeToClass[T]).asScala
    }

    @Nullable
    def getDataSource: DataSource = jt.getDataSource

    @throws(classOf[DataAccessException])
    def queryForSeq[T: ClassTag](sql: String, args: Seq[Any], argTypes: Seq[Int]): Seq[T] = {
      jt.queryForList(sql,
        asInstanceOfAnyRef(args).toArray,
        argTypes.toArray,
        typeToClass[T]).asScala
    }


    @throws(classOf[DataAccessException])
    def queryForSeq[T: ClassTag](sql: String, args: Any*): Seq[T] = {
      jt
        .queryForList(sql, typeToClass[T], asInstanceOfAnyRef(args): _*)
        .asScala
    }

    @throws(classOf[DataAccessException])
    def queryForRowSet(sql: String, args: Seq[Any], argTypes: Seq[Int]): SqlRowSet = {
      jt.queryForRowSet(sql, asInstanceOfAnyRef(args).toArray, argTypes.toArray)
    }

    @throws(classOf[DataAccessException])
    def queryForRowSet(sql: String, args: Any*): SqlRowSet = {
      jt.queryForRowSet(sql, asInstanceOfAnyRef(args): _*)
    }

    /**
     * Helper function for Java to Scala type conversion
     *
     * @param map
     * @return
     */
    private def asInstanceOfAny(map: java.util.Map[String, AnyRef]) = {
      map.asScala.toMap.mapValues(_.asInstanceOf[Any])
    }

    /**
     * Helper function for Java to Scala type conversion
     *
     * @param seq
     * @return
     */
    private def asInstanceOfAnyRef(seq: Seq[Any]): Seq[AnyRef] = {
      seq.map(_.asInstanceOf[AnyRef])
    }

    def tagToClass[T](tag: ClassTag[T]): Class[T] = {
      tag.runtimeClass.asInstanceOf[Class[T]]
    }

    def typeToClass[T: ClassTag]: Class[T] = {
      tagToClass(classTag[T])
    }
  }
}