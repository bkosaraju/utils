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

import java.io.File

import io.github.bkosaraju.utils.AppInterface
import org.apache.commons.io.FileUtils

trait cryptoSuiteTests extends AppInterface {
import commonUtils._

  test("encryptString :  Encrypt String") {
    assertResult("oYOgFi4gpi27FOxF7fBLLw==") {
      encryptString("something", "somekey")
    }
  }


  test("decryptString :  Decrypt String") {
    assertResult("something") {
      decryptString("oYOgFi4gpi27FOxF7fBLLw==", "somekey")
    }
  }


  private val plainFile = File.createTempFile("plainText", ".properties")
  private val decFile = File.createTempFile("encrypted", ".properties")
  private val returnFile = File.createTempFile("decrypted", ".properties")

  FileUtils.writeStringToFile(plainFile, "SomeDataToBeEncrypted")

  test("encryptFile  : Encrypt  File") {
    assertResult(true) {
      encryptFile(plainFile.toString, decFile.toString, "SomeRandomkey")
      FileUtils.readFileToString(decFile).ne(FileUtils.readFileToString(plainFile))
    }
  }

  test("decryptFile : Decrypt File") {
    assertResult("SomeDataToBeEncrypted") {
      decryptFile(decFile.toString, returnFile.toString, "SomeRandomkey")
      FileUtils.readFileToString(returnFile)
    }
  }

  test("decrypt : Raise an exception in case if it cant be decrypted") {
    intercept[Exception] {
      decryptFile(decFile.toString, returnFile.toString, "SomeOtherRandomeTry")
      FileUtils.readFileToString(returnFile)
    }
  }

  test("getKey : Raise an exception in case if it cant generate key") {
    intercept[Exception] {
      getKey(null)
    }
  }


  test("encryptFile : Raise an exception in case if it cant encrypt File") {
    intercept[Exception] {
      encryptFile(null, null, null)
    }
  }


  test("decryptFile : Raise an exception in case if it cant decrypt File") {
    intercept[Exception] {
      decryptFile(null, null, null)
    }
  }


  test("encryptString : Raise an exception in case if it cant encrypt string") {
    intercept[Exception] {
      encryptString(null, null)
    }
  }


  test("decryptString : Raise an exception in case if it cant decrypt string") {
    intercept[Exception] {
      decryptString(null, null)
    }
  }


  test("crypt : Raise an exception in case if there is any issue with crypting the stream") {
    intercept[Exception] {
      crypt(null, 111111, null)
    }
  }


  test("encrypt : Raise an exception in case if it cant be encrypted") {
    intercept[Exception] {
      encrypt(null, null)
    }
  }

  test("decrypt  : Raise an exception in case if it cant be decrypted") {
    intercept[Exception] {
      decrypt(null, null)
    }
  }

}
