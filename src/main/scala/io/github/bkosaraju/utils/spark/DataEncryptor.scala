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

import io.github.bkosaraju.utils.common.{CryptoSuite, Session}
import io.github.bkosaraju.utils.common.{CryptoSuite, Session}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}

object DataEncryptor extends Serializable with Session with CryptoSuite {

  /**
   * == DataEncryptor Implicit class which extends base dataframe class ==
   */
  implicit class DataEncryptor(var df: DataFrame) {
    /**
     * encryptColumn Wrapper
     *
     * @return
     */

    val encryptCol: ((String, String) => String) = { (col, key) =>
      if (col == null) {
        null
      } else
        encryptString(col, key)
    }
    /**
     * decryptColumn Wrapper
     *
     * @return
     */

    val decryptCol: ((String, String) => String) = { (col, key) =>
      if (col == null) {
        null
      } else {
        decryptString(col, key)
      }
    }

    def encryptColumn: UserDefinedFunction = udf(encryptCol)

    def decryptColumn: UserDefinedFunction = udf(decryptCol)

    /**
     * generate encrypted content for given column sequence and pad to same data frame as same column name
     *
     * @param columns       input Column list in sequence
     * @param encryptionKey string value of encryption key
     * @return Dataframe with encrypted  columns
     */
    def encrypt(encryptionKey: String, columns: String*): DataFrame = {
      try {
        columns.foldLeft(df)((sDF, clmn) => {
          sDF.withColumn(clmn, encryptColumn(col(clmn), lit(encryptionKey)))
        })
      } catch {
        case e: Exception => {
          logger.error(s"Unable to encrypt given cloumns(${columns}) from dataframe..")
          throw e
        }
      }
    }

    /**
     * generate decrypted content for given column sequence and pad to same data frame as same column name
     *
     * @param columns       input Column list in sequence
     * @param encryptionKey string value of encryption key
     * @return Dataframe with decrypted  columns
     */

    def decrypt(encryptionKey: String, columns: String*): DataFrame = {
      try {
        columns.foldLeft(df)((sDF, clmn) => {
          sDF.withColumn(clmn, decryptColumn(col(clmn), lit(encryptionKey)))
        })
      } catch {
        case e: Exception => {
          logger.error(s"Unable to decrypt given cloumns(${columns}) from dataframe..")
          throw e
        }
      }
    }
  }

}
