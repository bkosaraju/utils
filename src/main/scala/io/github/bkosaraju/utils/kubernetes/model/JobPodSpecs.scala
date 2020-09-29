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
import io.kubernetes.client.openapi.models.V1PodSpec
import collection.JavaConverters._

class JobPodSpecs(config: Map[String,String]) extends Config{

  def getJobPodSpecs: V1PodSpec = {
    try {
    val podSpecs = new V1PodSpec
    podSpecs.setContainers(List(SparkContainer(config)).asJava)
      if (config.getOrElse("persistentVolumeEnabled",PERSISTENT_VOLUME_ENABLED).equalsIgnoreCase("true")) {
        podSpecs.setVolumes(List(PersistantVolume(config)).asJava)
      }
    podSpecs.setServiceAccountName(config.getOrElse("podServiceAccount",POD_SERVICE_ACCOUNT))
    podSpecs.setRestartPolicy("Never")
    podSpecs.setSecurityContext(PodSecurityContext(config))
    podSpecs
  } catch  {
      case e: Exception => {
        logger.error("Unable to generate pod specs for given container..")
        throw e
      }
    }
  }
}

object JobPodSpecs {
  def apply(config: Map[String, String]): V1PodSpec = new JobPodSpecs(config).getJobPodSpecs
}
