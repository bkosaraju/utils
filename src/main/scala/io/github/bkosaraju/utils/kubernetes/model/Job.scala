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

package io.github.bkosaraju.utils.kubernetes.model

import io.github.bkosaraju.utils.kubernetes.Config
import io.kubernetes.client.openapi.models.V1Job

class Job(config: Map[String,String]) extends Config {

  def getJobDef : V1Job = {
    try {
      val job = new V1Job
      job.setApiVersion(config.getOrElse("apiVersion",KUBE_API_VERSION))
      job.setKind(config.getOrElse("podKind",KUBE_POD_KIND))
      job.setMetadata(JobMetadata(config))
      job.setSpec(JobContainerSpecs(config))
      job
    } catch  {
      case e : Exception => {
        logger.error("Unable to create Job with given specs")
        throw e
      }
    }
  }
}

object Job {
  def apply(config: Map[String, String]): V1Job = new Job(config).getJobDef
}
