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

package io.github.bkosaraju.utils.databricks.sdk

import io.github.bkosaraju.utils.aws.AwsUtils
import io.github.bkosaraju.utils.common.Session
import io.github.bkosaraju.utils.databricks.model.{ResponeseJobSubmit, S3OutputLogs}
import io.github.bkosaraju.utils.interface.Aws
import com.amazonaws.services.s3.AmazonS3URI
import io.github.bkosaraju.utils.common.Session

class GetLogs (config : Map[String,String] ) extends Utils(config) with Session {

  def driverLogURI( responeseJobSubmit: ResponeseJobSubmit ) : S3OutputLogs = {
    val baseClusterURI = GetStatus(config, responeseJobSubmit)
      .getClusterSpec
      .getNewCluster
      .getClusterLogConf
      .getS3
      .getDestination + GetStatus(config, responeseJobSubmit).getClusterInstance.getClusterId + "/driver"
    S3OutputLogs(
      baseClusterURI + LOG_S3_DRIVER_LOGNAME,
      baseClusterURI + LOG_S3_DRIVER_STDOUT,
      baseClusterURI + LOG_S3_DRIVER_STDERR)
  }

  def getLogsAsStirng ( responeseJobSubmit: ResponeseJobSubmit ) : String = {
    var retryCounter, cntr = config.getOrElse("s3_log_retry_counter",LOG_RETRY_COUNTER).toString.toInt
    try {
      val s3Client = (new AwsUtils).getS3Client()
      val driverLogs = driverLogURI(responeseJobSubmit)
      val printLogs = new StringBuilder
      val s3DriverLogURI = new AmazonS3URI(driverLogs.getDriverLog)
      val s3DriverErrURI = new AmazonS3URI(driverLogs.getDriverstderr)
      val s3DriverOutURI = new AmazonS3URI(driverLogs.getDriverstdOut)
      printLogs.append(s"""\n${"=" * 15}Driver Log${"=" * 15}\n""")
      printLogs.append(s3Client.getObjectAsString(s3DriverLogURI.getBucket, s3DriverLogURI.getKey))
      printLogs.append(s"""\n${"=" * 15}stdOut${"=" * 15}\n""")
      printLogs.append(s3Client.getObjectAsString(s3DriverOutURI.getBucket, s3DriverOutURI.getKey))
      printLogs.append(s"""\n${"=" * 15}stdErr${"=" * 15}\n""")
      printLogs.append(s3Client.getObjectAsString(s3DriverErrURI.getBucket, s3DriverErrURI.getKey))
      printLogs.toString()
    } catch {
      case e : com.amazonaws.services.s3.model.AmazonS3Exception =>  {
        if (e.getLocalizedMessage.contains("The specified key does not exist") && retryCounter > 0 ) {
          logger.info(s"Apparently logs not copied to S3 yet, hence waiting for another 30 seconds ..(${retryCounter} of ${cntr})")
          Thread.sleep(30000L)
          retryCounter -= 1
          getLogsAsStirng(responeseJobSubmit)
        } else {
          throw e
        }
      }
    }
  }
}

object GetLogs {
  def apply(config: Map[String, String],responeseJobSubmit: ResponeseJobSubmit): String =
    (new GetLogs(config)).getLogsAsStirng(responeseJobSubmit)
}