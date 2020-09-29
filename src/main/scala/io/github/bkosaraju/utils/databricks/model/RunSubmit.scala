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

package io.github.bkosaraju.utils.databricks.model

import java.io.Serializable
import java.util.List

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}

import scala.beans.BeanProperty

@SerialVersionUID(5673995339062370148L)
@JsonInclude(JsonInclude.Include.NON_NULL)
class RunSubmit extends Serializable {

  @JsonProperty("run_name")
  @BeanProperty
  var runName: String = _

  @JsonProperty("new_cluster")
  @BeanProperty
  var newCluster: NewCluster = _

  @JsonProperty("existing_cluster_id")
  @BeanProperty
  var existingClusterId: String = _

  @JsonProperty("libraries")
  @BeanProperty
  var libraries: List[Library] = _

  @JsonProperty("spark_submit_task")
  @BeanProperty
  var sparkSubmitTask: SparkSubmitTask = _

  @JsonProperty("spark_jar_task ")
  @BeanProperty
  var sparkJarTask: SparkJarTask = _

  @JsonProperty("notebook_task")
  @BeanProperty
  var notebookTask: NotebookTask = _

  @JsonProperty("spark_python_task")
  @BeanProperty
  var sparkPythonTask: SparkPythonTask = _

  @JsonProperty("timeout_seconds")
  @BeanProperty
  var timeOutSeconds: Int = _

}