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

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConverters._

import io.github.bkosaraju.utils.aws.AwsUtils
import com.amazonaws.services.s3.AmazonS3URI

trait LoadProperties {

  implicit class LoadProperties(inputProps: String) extends Session {

    def loadParms(appProps: String = "app.properties"): Properties = {
      var props = loadLocalappProps(appProps)
      if (!inputProps.isEmpty) {
        if (inputProps.contains("s3a:") || inputProps.contains("s3:")) {
          val s3Client = (new AwsUtils).getS3Client()
          val S3URI = new AmazonS3URI(inputProps.replaceAll("s3a:", "s3:"))
          props.load(s3Client.getObject(S3URI.getBucket, S3URI.getKey).getObjectContent)
        } else {
          props.load(new FileInputStream(inputProps))
        }
      }
      decryptSecretKeys(props)
    }

    /**
     * loads Internal properties as baseline for application.
     *
     * @return props java.util.properties
     * {{{
     *                val internalpops = getClass.getClassLoader.getResourceAsStream(appProps)
     *                prop.load(internalpops)
     *
     * }}}
     */
    def loadLocalappProps(appProps: String = "app.properties"): Properties = {

      val prop = new Properties()
      try {
        val internalpops = getClass.getClassLoader.getResourceAsStream(appProps)
        prop.load(internalpops)
        prop
      } catch {
        case
          e: Throwable =>
          logger.warn("Unable to load local properties", e)
      }
      prop
    }

    /**
     * Reads secrets from properties and get the associative value from secret stores
     * ex : {{{
     *   "secret.somekey" : "secret store key"
     *    will translate to  -- somekey : result_values
     * }}}
     * @param props
     * @return Properties
     */
    def decryptSecretKeys(props: Properties): Properties = {
      val secureProps = props
        .stringPropertyNames()
        .asScala
        .filter(_.matches("^secret\\..*"))
      if (secureProps.nonEmpty) {
        val awsUtils = (new AwsUtils)
        awsUtils.setConfig(props.asInstanceOf[java.util.Map[String,String]].asScala.toMap)
        secureProps
          .map(x => props.setProperty(
            x.replaceAll("^secret\\.", ""),
            awsUtils.getSSMValue(props.getProperty(x))
          )
          )
      }
      props
    }
  }

}
