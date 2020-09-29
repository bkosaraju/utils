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

import org.apache.spark.sql.types.{StringType, StructField, StructType}

trait SchemaComparison extends Context {
  /** Validates whether both given Schemas are exactly the same or not
   *
   * @note while comparing the source and target schemas both column names are converted to lower case and datatype coparision is ignored.
   *       this is to ensure that for schema infer datasets as the datatype converter will take care of the datatype conversions.
   * @return boolean true incase both of them are same else false.
   * @example isSchemaSame(scheam1,schema2)
   * @param schema1       first schema
   * @param schema2       second schema
   *
   *                  {{{
   *                      if (sortAndConv(schema1).equals(sortAndConv(schema2))) true else false
   *                           }}}
   *                  */
  def isSchemaSame(schema1: StructType, schema2: StructType): Boolean = {
    /* Sort the columns and convert the case of the column names in along with defaulting the datatype.
      *
      * @note while comparing the source and target schemas both column names are converted to lower case and datatype coparision is ignored.
      *       this is to ensure that for schema infer datasets as the datatype converter will take care of the datatype conversions.
      * @return Seq[StructField].
      * @example sortAndConv(scheam)
      * @param Schema Strunct Type Schema
      *
      *                  {{{
      * Schema.map(x => x.copy(name = x.name.toLowerCase, dataType = StringType, nullable = true)).sortBy(_.name)
      *                           }}}
      *                  */
    def sortAndConv(Schema: StructType): Seq[String] = {
      try {
        if (Schema == null) { Seq() } else { Schema.map(x =>  (x.name.toLowerCase)).sorted.toSeq }
      }catch {
        case e : Exception => logger.error("Unable to Validate Source and/or Target Schema",e)
          throw e
      }
    }

    try { sortAndConv(schema1) forall sortAndConv(schema2).contains } catch {
      case e : Exception => logger.error("Source and Target Schemas Do Not Match",e)
        throw e
    }
  }
}