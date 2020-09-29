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

import java.io._
import java.util.Base64

import javax.crypto.spec.SecretKeySpec
import javax.crypto.{Cipher, CipherInputStream}
import org.apache.commons.io.IOUtils

trait CryptoSuite extends Session {

  val ALGORITHM = "AES"

  /**
   * Method to generate key which is basically compute the user provided string and extract the sha256 of it
   * then chop first 32 bytes of the string as encrypt or decrypt key
   *
   * @param key - Ascii string
   * @return - Ascii string
   * {{{
   *         new String(org.apache.commons.codec.digest.DigestUtils.sha256Hex(key)).substring(0, 32)
   * }}}
   */
  def getKey(key: String): String = {
    try {
      new String(org.apache.commons.codec.digest.DigestUtils.sha256Hex(key)).substring(0, 32)
    } catch {
      case e: Exception => logger.error("Unable to Generate sha256 for Given Input Key", e)
        throw e
    }
  }

  /**
   * Method to convert the given input stream into crypted(encrypt) input stream
   *
   * @param key - Ascii string
   * @param is  - Input Stream
   * @return - InuputStream
   * {{{
   *          crypt(key, Cipher.ENCRYPT_MODE,is)
   * }}}
   */
  def encrypt(key: String, is: InputStream): InputStream = {
    try {
      crypt(key, Cipher.ENCRYPT_MODE, is)
    } catch {
      case e: Exception =>
        logger.error("Unable to Encrypt Input Data ..", e)
        throw e
    }
  }

  /**
   * Method to convert the given input stream into crypted(decrypt) input stream
   *
   * @param key - Ascii string
   * @param is  - Input Stream
   * @return - InuputStream
   * {{{
   *          crypt(key, Cipher.DECRYPT_MODE, is )
   * }}}
   */
  def decrypt(key: String, is: InputStream): InputStream = {
    try {
      crypt(key, Cipher.DECRYPT_MODE, is)
    } catch {
      case e: Exception =>
        logger.error("Unable to Decrypt Input Data ..", e)
        throw e
    }
  }

  /**
   * Method to Encrypt given Input file and write the data into output file
   *
   * @param inputFileName  - Input file name
   * @param outputFileName - Output file name
   * @param key            - Ascii string
   * @return - None
   * {{{
   *                val ip = new FileInputStream(new File(inputFileName))
   *                val cryptStream = encrypt(key, ip)
   *                val os = new FileOutputStream(new File(outputFileName))
   *                os.write(IOUtils.toByteArray(cryptStream))
   * }}}
   */
  def encryptFile(inputFileName: String, outputFileName: String, key: String): Unit = {
    try {
      val ip = new FileInputStream(new File(inputFileName))
      val cryptStream = encrypt(key, ip)
      val os = new FileOutputStream(new File(outputFileName))
      os.write(IOUtils.toByteArray(cryptStream))
      os.flush()
      os.close()
      ip.close()
    } catch {
      case e: Exception => logger.error("Unable to Encrypt Given File", e)
        throw e
    }
  }

  /**
   * Method to convert the given input String into encrypted String with base64 encoding
   *
   * @param inputString - input String
   * @param key         - Ascii string
   * @return - base64 vale of encrypted string that represented in ascii format
   * {{{
   *         val in: InputStream = IOUtils.toInputStream(inputString, "UTF-8")
   *         Base64.getEncoder.encodeToString(
   *         IOUtils.toByteArray(encrypt(key, in))
   * }}}
   */
  def encryptString(inputString: String, key: String): String = {
    try {
      val in: InputStream = IOUtils.toInputStream(inputString, "UTF-8")
      Base64.getEncoder.encodeToString(
        IOUtils.toByteArray(encrypt(key, in))
      )
    } catch {
      case e: Exception => logger.error("Unable to Encrypt Given String", e)
        throw e
    }
  }

  /**
   * Method to convert the given input String(base64 vale of input value) into decrypted String in ascii format
   *
   * @param encryptedString - input(encrypted) String
   * @param key             - Ascii string
   * @return - Ascii string
   * {{{
   *          val inputBytes = Base64.getDecoder.decode(encryptedString)
   *          val eis = new ByteArrayInputStream(inputBytes)
   *          IOUtils.toString(decrypt(key,eis))
   * }}}
   */
  def decryptString(encryptedString: String, key: String): String = {
    try {
      val inputBytes = Base64.getDecoder.decode(encryptedString)
      val eis = new ByteArrayInputStream(inputBytes)
      IOUtils.toString(decrypt(key, eis))
    } catch {
      case e: Exception => logger.error("Unable to Decrypt Given String", e)
        throw e
    }
  }

  /**
   * Method to Decrypt given Input file and write the data into output file
   *
   * @param inputFileName  - Input file name
   * @param outputFileName - Output file name
   * @param key            - Ascii string
   * @return - None
   * {{{
   *         val ip = new FileInputStream(new File(inputFileName))
   *         val cryptStream = decrypt(key,ip)
   *         val os = new FileOutputStream(new File(outputFileName))
   *         os.write(IOUtils.toByteArray(cryptStream))
   * }}}
   */
  def decryptFile(inputFileName: String, outputFileName: String, key: String): Unit = {
    try {
      val ip = new FileInputStream(new File(inputFileName))
      val cryptStream = decrypt(key, ip)
      val os = new FileOutputStream(new File(outputFileName))
      os.write(IOUtils.toByteArray(cryptStream))
      os.flush()
      os.close()
      ip.close()
    } catch {
      case e: Exception => logger.error("Unable to Decrypt Given File", e)
        throw e
    }
  }

  /**
   * Central Method to encrypt and decrypt the given input stream and convert to output stream
   *
   * @param key  - Ascii string
   * @param mode - Integer value of crypto suite mode (encrypt or decrypt)
   * @param is   - Input stream
   * @return Inputstream of the crypted message
   * {{{
   *          val keySpecs = new SecretKeySpec(getKey(key).getBytes,ALGORITHM)
   *          val cipher = Cipher.getInstance(ALGORITHM)
   *          cipher.init(mode, keySpecs)
   *          new CipherInputStream(is, cipher)
   * }}}
   */
  def crypt(key: String, mode: Int, is: InputStream): InputStream = {
    try {
      val keySpecs = new SecretKeySpec(getKey(key).getBytes, ALGORITHM)
      val cipher = Cipher.getInstance(ALGORITHM)
      cipher.init(mode, keySpecs)
      new CipherInputStream(is, cipher)
    } catch {
      case e: Exception => logger.error("Unable to Crypt Given Input Stream", e)
        throw e
    }
  }
}
