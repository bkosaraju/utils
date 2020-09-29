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
import org.springframework.http.{HttpEntity, HttpMethod, ResponseEntity}
import org.springframework.web.util.UriComponentsBuilder

class GetStatus(config: Map[String,String]) extends Utils(config) {

  def getJobStatus(responeseJobSubmit: ResponeseJobSubmit): ResponseRunId = {
    val REST_END_POINT = config.getOrElse("DB_URL", DB_URL) + "/" + config.getOrElse("API_VERSION", API_VERSION) + JOB_RUN_GET_ENDPOINT
    val uriBuilder = UriComponentsBuilder
      .fromHttpUrl(REST_END_POINT)
      .queryParam("run_id", responeseJobSubmit.getRunId.toString)

    val requestEntity = new HttpEntity[AnyRef]("body", getAuthHeader)

    getTemplateforJson.exchange(
      uriBuilder.toUriString,
      HttpMethod.GET, requestEntity,
      classOf[ResponseRunId]).getBody
  }

  def getJobStatus(runId: Long): ResponseRunId = {
    val REST_END_POINT = config.getOrElse("DB_URL", DB_URL) + "/" + config.getOrElse("API_VERSION", API_VERSION) + JOB_RUN_GET_ENDPOINT
    val uriBuilder = UriComponentsBuilder
      .fromHttpUrl(REST_END_POINT)
      .queryParam("run_id", runId.toString)

    val requestEntity = new HttpEntity[AnyRef]("body", getAuthHeader)

    getTemplateforJson.exchange(
      uriBuilder.toUriString,
      HttpMethod.GET, requestEntity,
      classOf[ResponseRunId]).getBody
  }
}


  object GetStatus {

    def apply(config: Map[String,String], responeseJobSubmit: ResponeseJobSubmit) : ResponseRunId =  {
    (new GetStatus(config)).getJobStatus(responeseJobSubmit)
    }

    def apply(config: Map[String, String],runId : Long ): ResponseRunId = {
      (new GetStatus(config)).getJobStatus(runId)
    }
  }
