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

package io.github.bkosaraju.utils.kubernetes

import java.text.SimpleDateFormat

import io.github.bkosaraju.utils.aws.AwsUtils
import io.github.bkosaraju.utils.interface.Common
import io.kubernetes.client.openapi.apis.CoreV1Api

import scala.beans.BeanProperty

class UploadPodLog extends Common {

  @BeanProperty
  var s3Uri : String = ""

  @BeanProperty
  var appConfiguration : Map[String,String] = _

  def uploadPodLogToS3(podName: String, nameSpace: String, api: CoreV1Api): Unit = {
    try {
      if (appConfiguration == null) {
        logger.error("Must Pass Config to load to S3")
        throw InvalidS3MetadatException("Must Pass Config to load to S3")
      } else {
        logger.info("Uploading PodLog to S3 Location..")
        val s3config = collection.mutable.Map[String,String]() ++ appConfiguration
        if (s3Uri.nonEmpty) {
          s3config.put("s3Uri",s3Uri)
        }
        val awsUtils = new AwsUtils
        awsUtils.putS3ObjectFromString(s3config.toMap,
          api.readNamespacedPodLog(podName, nameSpace, null, null, null, null, null, null, null, null)
        )
        logger.info(s"Successfully uploaded log into : ${s3Uri}")
      }
    } catch {
      case e: Exception => {
        logger.error(s"Unable to Upload log Log from Pod(${podName} in namespace ${nameSpace}) - error while exporting log", e)
      }
    }
  }

}

object UploadPodLog extends Config {
  def apply(config: Map[String, String], podName: String, nameSpace: String, api: CoreV1Api): Unit = {
    val df = new SimpleDateFormat("/YYYY/MM/dd/")
    val ul = new UploadPodLog()
    ul.setAppConfiguration(config)
    val s3URI = config.getOrElse("s3LogDestination", LOG_S3_DESTINATION).trim.replaceAll("/$", "") + df.format(java.util.Calendar.getInstance().getTime())  + nameSpace + "/" + podName + ".log.gz"
    ul.setS3Uri(s3URI)
    ul.uploadPodLogToS3(podName, nameSpace, api)
  }
}
