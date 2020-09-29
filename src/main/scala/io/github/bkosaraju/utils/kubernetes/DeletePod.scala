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

import com.google.gson.JsonSyntaxException
import io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}

class DeleteConfig extends Config {

  def deleteJob(jobName: String, nameSpace: String, api: BatchV1Api): Unit = {
    try {
      logger.info(s"Trying to clean up the job($jobName)..")
      api.deleteNamespacedJob(jobName, nameSpace, null, null, null, null, null, null)
      logger.info(s"Successfully deleted job ${jobName}")
    } catch {
      case e: JsonSyntaxException => {
        if (e.getCause().isInstanceOf[IllegalStateException]) {
          val ise = e.getCause
          if (ise != null && ise.getMessage.contains("Expected a string but was BEGIN_OBJECT")) {
            logger.debug("Catching exception because of issue https://github.com/kubernetes-client/java/issues/86", e)
            logger.info(s"Successfully deleted job ${jobName}")
          } else throw e
        }
      }
      case e: Exception => {
        logger.error(s"Unable to delete Job: ${jobName} in ${nameSpace}", e)
      }
    }
  }

  def deletePod(podName: String, nameSpace: String, api: CoreV1Api): Unit = {
    try {
      logger.info(s"Trying to clean up the pod (${podName})..")
      api.deleteNamespacedPod(podName, nameSpace, null, null, null, null, null, null)
      logger.info(s"Successfully deleted pod -  ${podName}")
    } catch {
      case e: JsonSyntaxException => {
        if (e.getCause().isInstanceOf[IllegalStateException]) {
          val ise = e.getCause
          if (ise != null && ise.getMessage.contains("Expected a string but was BEGIN_OBJECT")) {
            logger.debug("Catching exception because of issue https://github.com/kubernetes-client/java/issues/86", e)
            logger.info(s"Successfully deleted pod -  ${podName}")
          } else throw e
        }
      }
      case e: Exception => {
        logger.error(s"Unable to delete Pod : ${podName} in ${nameSpace}", e)
      }
    }
  }
}

object DeleteConfig {
  def apply(): DeleteConfig = new DeleteConfig()
}
