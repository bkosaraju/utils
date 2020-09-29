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
import io.kubernetes.client.openapi.models.V1PodSecurityContext

class PodSecurityContext(config: Map[String,String]) extends Config {
def getPodSecurityContext : V1PodSecurityContext =  {
  try {
    val podSecurityContext = new V1PodSecurityContext()
    podSecurityContext.setFsGroup(config.getOrElse("securityContextFsGroup", SECURITY_CONTEXT_FSGROUP).toString.toLong)
    podSecurityContext
  } catch {
    case e : Exception => {
      logger.error("Unable to generate pod securty context..")
      throw e
    }
  }
}
}

object PodSecurityContext {
  def apply(config: Map[String, String]): V1PodSecurityContext = new PodSecurityContext(config).getPodSecurityContext
}
