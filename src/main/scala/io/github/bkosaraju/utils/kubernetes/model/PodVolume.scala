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

import io.kubernetes.client.openapi.models.V1VolumeMount
import io.github.bkosaraju.utils.kubernetes.Config

class PodVolume( config : Map[String,String] ) extends Config {

  def getPodVolume : V1VolumeMount = {
    try {
      val volumeMount = new V1VolumeMount
      volumeMount.setName(config.getOrElse("volumeMountName", VOLUME_MOUNT_NAME))
      volumeMount.setMountPath(config.getOrElse("volumeMountPath", VOLUME_MOUNT_PATH))
      volumeMount
    } catch {
      case e: Exception => {
        logger.error("Unable to get Pod Volume mount so that it can mount to target Mount")
        throw e
      }}
  }
}

object PodVolume {
  def apply(config: Map[String, String]): V1VolumeMount = new PodVolume(config).getPodVolume
}




