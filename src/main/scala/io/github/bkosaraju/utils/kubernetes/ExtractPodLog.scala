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

import io.kubernetes.client.openapi.apis.CoreV1Api

class ExtractPodLog extends Config {
  def extractPodLog(podName: String, nameSpace: String, api: CoreV1Api): Unit = {
    try {
      logger.info(
        api.readNamespacedPodLog(podName, nameSpace, null, null, null, null, null, null, null, null)
      )
    } catch {
      case e: Exception => {
        logger.error(s"Unable to extract Log from Pod(${podName} in namespace ${nameSpace}) - error while running Pod", e)
      }
    }
  }
}

object ExtractPodLog {
  def apply(podName: String, nameSpace: String, api: CoreV1Api): Unit = new ExtractPodLog().extractPodLog(podName, nameSpace, api)
}

