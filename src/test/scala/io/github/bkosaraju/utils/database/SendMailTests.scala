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

package io.github.bkosaraju.utils.database

import io.github.bkosaraju.utils.AppInterface

trait SendMailTests extends AppInterface {
  import commonUtils._
  private  val mp = Map(
    "mail.smtp.host"-> "smtp.office365.com",
    "mail.smtp.port" -> "587",
    "mail.smtp.auth" -> "true",
    "mail.smtp.starttls.enable"-> "true",
    "mail.smtp.user" -> "",
    "mail.smtp.password" -> "",
    "fromAddress" -> "someuser@somecompany.com",
    "toAddress" -> "someuser@somecompany.comu",
    "ccAddress" -> "someuser@somecompany.comu",
    "bccAddress" -> "someuser@somecompany.comu",
    "subject"-> "taxi data Message",
    "content" ->"This is trip data dict",
    //"attachments" -> "C:\\Users\\kosab1\\Downloads\\data_dictionary_trip_records_green.pdf",
    "mail.smtp.socks.host" -> "localhost",
    "mail.smtp.socks.port" -> "5555"
  )
  private val cntnt = Map("content" -> """<table>
                   | <tr>
                   |  <th>Name</th>
                   |  <th>Favorite Color</th>
                   | </tr>
                   | <tr>
                   |  <td>Bob</td>
                   |  <td>Yellow</td>
                   | </tr>
                   | <tr>
                   |  <td>Michelle</td>
                   |  <td>Purple</td>
                   | </tr>
                   |</table>
                   |""".stripMargin)

  test("SendMail : Able to send mail(raise exception for misconfiguration)..") {
    intercept[Exception] {
  //  assertResult(){
    sendMail(mp)
    }
  }


  //
  test("SendMail : Able to send mail in HTML format(raise exception for misconfiguration)..") {
    intercept[Exception] {
  //  assertResult(){
      sendMail(mp ++ cntnt)
    }
  }

  test("SendMail : send mail from exception..") {
    val customMap : Map[String,String]= Map (
      "jobId" -> "1000",
      "procId" -> "2000",
      "jobExecutionId" -> "199999",
      "procExecutionId" -> "22222"
    )
    val exception =  NonZeroReturnCodeError("Job generation non zero return code..")
    intercept[Exception] {
    //assertResult() {
      sendMailFromError( exception, mp ++ customMap)
    }
  }

  test("SendMail : Able to send mail(raise exception for misconfiguration non auth)..") {
    intercept[Exception] {
      sendMail(mp.filter(_._1 != "mail.smtp.auth"))
    }
  }
}

