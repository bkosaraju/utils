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

import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.util.ClientBuilder
import io.kubernetes.client.util.credentials.AccessTokenAuthentication

class KubeClient {
  def getClient(config: Map[String, String]): ApiClient = {
    val kubeSystem = EKSCluster(config)
    (new ClientBuilder)
      .setBasePath(kubeSystem.getEndpoint)
      .setAuthentication(
        new AccessTokenAuthentication(GetKubeToken(config)))
      .setVerifyingSsl(false)
      .setCertificateAuthority(kubeSystem.getCertificateAuthority.getData.getBytes)
      .build()
  }
}

object KubeClient {
  def apply(config: Map[String, String] = Map[String,String]()): ApiClient = (new KubeClient).getClient(config)
}

