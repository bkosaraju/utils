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

import io.github.bkosaraju.utils.databricks.model.{ResponeseJobSubmit, ResponseRunId}
import org.springframework.http.{HttpEntity, HttpMethod}

class SubmitJob(config: Map[String,String]) extends Utils(config) {

  def submitJob  : ResponeseJobSubmit = {
    val REST_END_POINT = config.getOrElse("DB_URL",DB_URL) +"/"+ config.getOrElse("API_VERSION",API_VERSION) + JOBS_SUBMIT_ENDPOINT
    val requestEntity = new HttpEntity[AnyRef](getNewClusterJobDef,getAuthHeader)
    val restTemplate = getTemplateforJson
    restTemplate.exchange(REST_END_POINT,HttpMethod.POST,requestEntity,classOf[ResponeseJobSubmit]).getBody
  }
}

object SubmitJob {
  def apply(config: Map[String,String]) : ResponeseJobSubmit =  { (new SubmitJob(config)).submitJob }
}