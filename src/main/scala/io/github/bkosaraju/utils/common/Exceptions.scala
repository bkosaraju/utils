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

package io.github.bkosaraju.utils.common

trait Exceptions {
  case class UnknownResourceURL (message: String ) extends  Exception(message)
  case class PatternMismatchException (message: String ) extends  Exception(message)
  case class UnknownDriverClass(message: String) extends Exception(message)
  case class UnknownDateFormatException(message: String) extends Exception(message)
  case class InvalidS3MetadatException(message: String) extends Exception(message)
  case class InvalidAuthenticationProvider(message: String) extends Exception(message)
  case class NoScriptOrCommandProvided(message: String) extends Exception(message)
  case class NonZeroReturnCodeError(message: String) extends Exception(message)
  case class TooLongToCompleteJob(message: String) extends Exception(message)
  case class UnknownJobStateReported(message: String) extends Exception(message)
  case class InvalidArgumentsPassed(message : String) extends Exception(message)
  case class UnableToDownloadLogs(message : String) extends Exception(message)
  case class NonExistedEMRCluster(message : String) extends Exception(message)
}
