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
import io.kubernetes.client.openapi.models.V1Container
import collection.JavaConverters._

class SparkContainer(config : Map[String,String]) extends Config {

  def getContainer : V1Container = {
    try {
      val container = new V1Container
      container.setName(config.getOrElse("sparkContainerName",SPARK_CONTAINER_NAME).replaceAll("_","-").replaceAll("\\.","-"))
      container.setImage(config.getOrElse("containerImage",SPARK_CONTAINER_IMAGE))
      container.command(SparkCommandBuilder(config))
      if (config.getOrElse("persistentVolumeEnabled",PERSISTENT_VOLUME_ENABLED).equalsIgnoreCase("true")) {
        container.setVolumeMounts(List(PodVolume(config)).asJava)
      }
      container
  } catch {
      case e: Exception => {
      logger.error("Unable to generate Container configuration")
      throw e
      }
    }
  }
}

object SparkContainer {
  def apply(config: Map[String, String]): V1Container = new SparkContainer(config).getContainer
}
