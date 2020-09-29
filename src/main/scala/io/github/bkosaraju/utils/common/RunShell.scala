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

import scala.sys.process.{Process, ProcessLogger}

trait RunShell extends Session with Exceptions {
  def runShell(config: Map[String, String]): Unit = {
    val stdOutput, stdError = new StringBuilder

    val processConfig = sys.env ++ config + ("PATH" -> (config.getOrElse("PATH", "") + ":" + sys.env.getOrElse("PATH", "")))
    val pLogger = ProcessLogger(
      (o: String) => {
        stdOutput.append(System.getProperty("line.separator"))
        stdOutput.append(o)
      },
      (e: String) => {
        stdError.append(System.getProperty("line.separator"))
        stdError.append(e)
      })
    try {
      val ps =
        if (config.contains("scriptLocation") || config.contains("command")) {
          Process(Seq(processConfig("SHELL"), "-c", config.getOrElse("scriptLocation", config("command"))), None, processConfig.toArray: _*).run(pLogger)
        } else {
          logger.error("Unknown arguments passed(must pass scriptLocation or command) for Process execution ..")
          throw NoScriptOrCommandProvided("Unknown arguments passed(must pass scriptLocation or command) for Process execution ..")
        }
      if (ps.exitValue() == 0) {
        logger.info("Script executed successfully ..")
        logger.info("\n===============stdOut===============")
        logger.info(stdOutput.toString())
        logger.info("\n===============stdErr===============")
        logger.info(stdError.toString())
      } else {
        logger.error(s"Script executed with non zero return code(${ps.exitValue()})")
        logger.info("\n===============stdOut===============")
        logger.error(stdOutput.toString())
        logger.info("\n===============stdErr===============")
        logger.error(stdError.toString())
        throw NonZeroReturnCodeError(s"Script executed with non zero return code(${ps.exitValue()})")
      }
    } catch {
      case e: Exception => {
        logger.error("Unable to execute command")
        throw e
      }
    }
  }
}
