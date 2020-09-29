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
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty, JsonPropertyOrder}
import scala.beans.BeanProperty

@SerialVersionUID(5433714287896145617L)
@JsonInclude(JsonInclude.Include.NON_NULL)
class SparkJarTask extends Serializable {

  @BeanProperty
  @JsonProperty("jar_uri")
  var jarUri: String = _

  @BeanProperty
  @JsonProperty("main_class_name")
  var mainClassName: String = _

  @BeanProperty
  var parameters: Array[String] = _

  @BeanProperty
  @JsonProperty("run_as_repl")
  var runAsRepl: Boolean = _
  
}