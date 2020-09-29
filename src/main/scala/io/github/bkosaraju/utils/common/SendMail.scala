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

import java.io.File
import java.util.{Properties, UUID}

import javax.activation.{DataHandler, FileDataSource}
import javax.mail.Message.RecipientType.{BCC, CC, TO}
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.{Authenticator, PasswordAuthentication, Transport}
import org.apache.commons.io.FileUtils
import collection.JavaConverters._
import javax.mail.{Session => ses}

trait SendMail extends Session with StringToMap {
  /**
   * Mail Application which sends E-mails with status
   *
   * @param config
   * supported configuration
   * {{{
   * Name 	Type 	Description
   * mail.smtp.user 	String 	Default user name for SMTP.
   * mail.smtp.host 	String 	The SMTP server to connect to.
   * mail.smtp.port 	int 	The SMTP server port to connect to, if the connect() method doesn't explicitly specify one. Defaults to 25.
   * mail.smtp.connectiontimeout 	int 	Socket connection timeout value in milliseconds. This timeout is implemented by java.net.Socket. Default is infinite timeout.
   * mail.smtp.timeout 	int 	Socket read timeout value in milliseconds. This timeout is implemented by java.net.Socket. Default is infinite timeout.
   * mail.smtp.writetimeout 	int 	Socket write timeout value in milliseconds. This timeout is implemented by using a java.util.concurrent.ScheduledExecutorService per connection that schedules a thread to close the socket if the timeout expires. Thus, the overhead of using this timeout is one thread per connection. Default is infinite timeout.
   * mail.smtp.from 	String 	Email address to use for SMTP MAIL command. This sets the envelope return address. Defaults to msg.getFrom() or InternetAddress.getLocalAddress(). NOTE: mail.smtp.user was previously used for this.
   * mail.smtp.localhost 	String 	Local host name used in the SMTP HELO or EHLO command. Defaults to InetAddress.getLocalHost().getHostName(). Should not normally need to be set if your JDK and your name service are configured properly.
   * mail.smtp.localaddress 	String 	Local address (host name) to bind to when creating the SMTP socket. Defaults to the address picked by the Socket class. Should not normally need to be set, but useful with multi-homed hosts where it's important to pick a particular local address to bind to.
   * mail.smtp.localport 	int 	Local port number to bind to when creating the SMTP socket. Defaults to the port number picked by the Socket class.
   * mail.smtp.ehlo 	boolean 	If false, do not attempt to sign on with the EHLO command. Defaults to true. Normally failure of the EHLO command will fallback to the HELO command; this property exists only for servers that don't fail EHLO properly or don't implement EHLO properly.
   * mail.smtp.auth 	boolean 	If true, attempt to authenticate the user using the AUTH command. Defaults to false.
   * mail.smtp.auth.mechanisms 	String 	If set, lists the authentication mechanisms to consider, and the order in which to consider them. Only mechanisms supported by the server and supported by the current implementation will be used. The default is "LOGIN PLAIN DIGEST-MD5 NTLM", which includes all the authentication mechanisms supported by the current implementation except XOAUTH2.
   * mail.smtp.auth.login.disable 	boolean 	If true, prevents use of the AUTH LOGIN command. Default is false.
   * mail.smtp.auth.plain.disable 	boolean 	If true, prevents use of the AUTH PLAIN command. Default is false.
   * mail.smtp.auth.digest-md5.disable 	boolean 	If true, prevents use of the AUTH DIGEST-MD5 command. Default is false.
   * mail.smtp.auth.ntlm.disable 	boolean 	If true, prevents use of the AUTH NTLM command. Default is false.
   * mail.smtp.auth.ntlm.domain 	String 	The NTLM authentication domain.
   * mail.smtp.auth.ntlm.flags 	int 	NTLM protocol-specific flags. See http://curl.haxx.se/rfc/ntlm.html#theNtlmFlags for details.
   * mail.smtp.auth.xoauth2.disable 	boolean 	If true, prevents use of the AUTHENTICATE XOAUTH2 command. Because the OAuth 2.0 protocol requires a special access token instead of a password, this mechanism is disabled by default. Enable it by explicitly setting this property to "false" or by setting the "mail.smtp.auth.mechanisms" property to "XOAUTH2".
   * mail.smtp.submitter 	String 	The submitter to use in the AUTH tag in the MAIL FROM command. Typically used by a mail relay to pass along information about the original submitter of the message. See also the setSubmitter method of SMTPMessage. Mail clients typically do not use this.
   * mail.smtp.dsn.notify 	String 	The NOTIFY option to the RCPT command. Either NEVER, or some combination of SUCCESS, FAILURE, and DELAY (separated by commas).
   * mail.smtp.dsn.ret 	String 	The RET option to the MAIL command. Either FULL or HDRS.
   * mail.smtp.allow8bitmime 	boolean 	If set to true, and the server supports the 8BITMIME extension, text parts of messages that use the "quoted-printable" or "base64" encodings are converted to use "8bit" encoding if they follow the RFC2045 rules for 8bit text.
   * mail.smtp.sendpartial 	boolean 	If set to true, and a message has some valid and some invalid addresses, send the message anyway, reporting the partial failure with a SendFailedException. If set to false (the default), the message is not sent to any of the recipients if there is an invalid recipient address.
   * mail.smtp.sasl.enable 	boolean 	If set to true, attempt to use the javax.security.sasl package to choose an authentication mechanism for login. Defaults to false.
   * mail.smtp.sasl.mechanisms 	String 	A space or comma separated list of SASL mechanism names to try to use.
   * mail.smtp.sasl.authorizationid 	String 	The authorization ID to use in the SASL authentication. If not set, the authentication ID (user name) is used.
   * mail.smtp.sasl.realm 	String 	The realm to use with DIGEST-MD5 authentication.
   * mail.smtp.sasl.usecanonicalhostname 	boolean 	If set to true, the canonical host name returned by InetAddress.getCanonicalHostName is passed to the SASL mechanism, instead of the host name used to connect. Defaults to false.
   * mail.smtp.quitwait 	boolean 	If set to false, the QUIT command is sent and the connection is immediately closed. If set to true (the default), causes the transport to wait for the response to the QUIT command.
   * mail.smtp.reportsuccess 	boolean 	If set to true, causes the transport to include an SMTPAddressSucceededException for each address that is successful. Note also that this will cause a SendFailedException to be thrown from the sendMessage method of SMTPTransport even if all addresses were correct and the message was sent successfully.
   * mail.smtp.socketFactory 	SocketFactory 	If set to a class that implements the javax.net.SocketFactory interface, this class will be used to create SMTP sockets. Note that this is an instance of a class, not a name, and must be set using the put method, not the setProperty method.
   * mail.smtp.socketFactory.class 	String 	If set, specifies the name of a class that implements the javax.net.SocketFactory interface. This class will be used to create SMTP sockets.
   * mail.smtp.socketFactory.fallback 	boolean 	If set to true, failure to create a socket using the specified socket factory class will cause the socket to be created using the java.net.Socket class. Defaults to true.
   * mail.smtp.socketFactory.port 	int 	Specifies the port to connect to when using the specified socket factory. If not set, the default port will be used.
   * mail.smtp.ssl.enable 	boolean 	If set to true, use SSL to connect and use the SSL port by default. Defaults to false for the "smtp" protocol and true for the "smtps" protocol.
   * mail.smtp.ssl.checkserveridentity 	boolean 	If set to true, check the server identity as specified by RFC 2595. These additional checks based on the content of the server's certificate are intended to prevent man-in-the-middle attacks. Defaults to false.
   * mail.smtp.ssl.trust 	String 	If set, and a socket factory hasn't been specified, enables use of a MailSSLSocketFactory. If set to "*", all hosts are trusted. If set to a whitespace separated list of hosts, those hosts are trusted. Otherwise, trust depends on the certificate the server presents.
   * mail.smtp.ssl.socketFactory 	SSLSocketFactory 	If set to a class that extends the javax.net.ssl.SSLSocketFactory class, this class will be used to create SMTP SSL sockets. Note that this is an instance of a class, not a name, and must be set using the put method, not the setProperty method.
   * mail.smtp.ssl.socketFactory.class 	String 	If set, specifies the name of a class that extends the javax.net.ssl.SSLSocketFactory class. This class will be used to create SMTP SSL sockets.
   * mail.smtp.ssl.socketFactory.port 	int 	Specifies the port to connect to when using the specified socket factory. If not set, the default port will be used.
   * mail.smtp.ssl.protocols 	string 	Specifies the SSL protocols that will be enabled for SSL connections. The property value is a whitespace separated list of tokens acceptable to the javax.net.ssl.SSLSocket.setEnabledProtocols method.
   * mail.smtp.ssl.ciphersuites 	string 	Specifies the SSL cipher suites that will be enabled for SSL connections. The property value is a whitespace separated list of tokens acceptable to the javax.net.ssl.SSLSocket.setEnabledCipherSuites method.
   * mail.smtp.starttls.enable 	boolean 	If true, enables the use of the STARTTLS command (if supported by the server) to switch the connection to a TLS-protected connection before issuing any login commands. If the server does not support STARTTLS, the connection continues without the use of TLS; see the mail.smtp.starttls.required property to fail if STARTTLS isn't supported. Note that an appropriate trust store must configured so that the client will trust the server's certificate. Defaults to false.
   * mail.smtp.starttls.required 	boolean 	If true, requires the use of the STARTTLS command. If the server doesn't support the STARTTLS command, or the command fails, the connect method will fail. Defaults to false.
   * mail.smtp.proxy.host 	string 	Specifies the host name of an HTTP web proxy server that will be used for connections to the mail server.
   * mail.smtp.proxy.port 	string 	Specifies the port number for the HTTP web proxy server. Defaults to port 80.
   * mail.smtp.proxy.user 	string 	Specifies the user name to use to authenticate with the HTTP web proxy server. By default, no authentication is done.
   * mail.smtp.proxy.password 	string 	Specifies the password to use to authenticate with the HTTP web proxy server. By default, no authentication is done.
   * mail.smtp.socks.host 	string 	Specifies the host name of a SOCKS5 proxy server that will be used for connections to the mail server.
   * mail.smtp.socks.port 	string 	Specifies the port number for the SOCKS5 proxy server. This should only need to be used if the proxy server is not using the standard port number of 1080.
   * mail.smtp.mailextension 	String 	Extension string to append to the MAIL command. The extension string can be used to specify standard SMTP service extensions as well as vendor-specific extensions. Typically the application should use the SMTPTransport method supportsExtension to verify that the server supports the desired service extension. See RFC 1869 and other RFCs that define specific extensions.
   * mail.smtp.userset 	boolean 	If set to true, use the RSET command instead of the NOOP command in the isConnected method. In some cases sendmail will respond slowly after many NOOP commands; use of RSET avoids this sendmail issue. Defaults to false.
   * mail.smtp.noop.strict 	boolean 	If set to true (the default), insist on a 250 response code from the NOOP command to indicate success. The NOOP command is used by the isConnected method to determine if the connection is still alive. Some older servers return the wrong response code on success, some servers don't implement the NOOP command at all and so always return a failure code. Set this property to false to handle servers that are broken in this way. Normally, when a server times out a connection, it will send a 421 response code, which the client will see as the response to the next command it issues. Some servers send the wrong failure response code when timing out a connection. Do not set this property to false when dealing with servers that are broken in this way.
   * }}}
   */
  def sendMail (config : Map[String,String]): Unit = {
    val props = new Properties()
    props.putAll(config.asJava)
    val session : javax.mail.Session =
    if (props.getProperty("mail.smtp.auth","false").equalsIgnoreCase("true")) {
      ses.getInstance(props,
      PasswordAuthenticator(props.getProperty("mail.smtp.user",""),props.getProperty("mail.smtp.password"))
      )
    }  else { ses.getInstance(props) }
    val message = new MimeMessage(session)
    message.setSender(new InternetAddress(props.getProperty("fromAddress","admin@data-enginnering.com")))
    message.setFrom(new InternetAddress(props.getProperty("fromAddress","admin@data-enginnering.com")))
    if (props.containsKey("toAddress")) {
      message.setRecipient(TO,
        new InternetAddress(props.getProperty("toAddress")))
    }
    if (props.containsKey("ccAddress")) {
      message.setRecipient(CC, new InternetAddress(props.getProperty("ccAddress")))
    }
    if (props.containsKey("bccAddress")) {
      message.setRecipient(BCC, new InternetAddress(props.getProperty("bccAddress")))
    }
    message.setSubject(props.getProperty("subject","Auto generated"))
    //message.setText(props.getProperty("content","NA"))

    val messageBodyPart = new MimeBodyPart()
    messageBodyPart.setContent(props.getProperty("content","NA"),"text/html; charset=utf-8")
    val mulitPart = new MimeMultipart()
    mulitPart.addBodyPart(messageBodyPart)

    if (config.contains("attachments")) {
      config("attachments").split(",").foreach{
      attachFileName => {
         addAttachment(mulitPart,attachFileName,attachFileName
          .replaceAll(".*/","")
          .replaceAll(".*\\\\",""))
      }
      }
    }
    message.setContent(mulitPart)
    //message.writeTo(System.out)
    Transport.send(message)
  }

  case class PasswordAuthenticator(username: String, password: String) extends Authenticator {
    override def getPasswordAuthentication(): PasswordAuthentication = {
      new PasswordAuthentication(username, password)
    }
  }

  def addAttachment(mlp: MimeMultipart, fileName : String, attachmentName: String ): Unit = {
    val source = new FileDataSource(fileName)
    val messageBodyPart = new MimeBodyPart()
    messageBodyPart.setDataHandler(new DataHandler(source))
    messageBodyPart.setFileName(attachmentName)
    mlp.addBodyPart(messageBodyPart)
  }

  def sendMailFromError(e : Exception, config: Map[String,String] = Map()): Unit = {

    val jobId = config.getOrElse("jobId","NA")
    val jobExecutionId = config.getOrElse("jobExecutionId","NA")
    val procId = config.getOrElse("procId","NA")
    val procExecutionId = config.getOrElse("procExecutionId","NA")
    val errorDetails = e.getMessage
    val props = new Properties()
    props.putAll(config.filterKeys( ! _.toLowerCase.contains("password")).asJava)
    val tmpDir = new File(FileUtils.getTempDirectory+"/"+UUID.randomUUID().toString.replaceAll("-",""))
      FileUtils.forceMkdir(tmpDir)
    import java.io.FileOutputStream
    val configFile = new File (tmpDir + "/app.config")
    val f = new FileOutputStream(configFile)
    //val s = new Output(f)
    props.store(f,null)
    f.close()
    val errorStackTrace = e.getStackTrace.mkString("\n")
    val jobSpecs = Map(
      "attachments" -> (
        if (config.contains("attachments")) {
          config("attachments") + "," + configFile
        } else configFile).toString,
      "subject" -> s"Failure - [${jobId}] - ${errorDetails}",
      "content" ->
    s"""<!DOCTYPE html>
      |<html>
      |<head>
      |<style>
      |table {
      |  font-family: calibri;
      |  border-collapse: collapse;
      |  width: 100%;
      |  color: 585656;
      |}
      |
      |td, th {
      |  border: 1px solid #dddddd;
      |  text-align: left;
      |  padding: 8px;
      |}
      |
      |tr:nth-child(even) {
      |  background-color: #dddddd;
      |}
      |</style>
      |</head>
      |<body>
      |
      |<h2>Failed Application Details:</h2>
      |
      |<table>
      |  <tr>
      |    <td>job_id</td>
      |    <td>${jobId}</td>
      |  </tr>
      |  <tr>
      |    <td>job_execution_id</td>
      |    <td>${jobExecutionId}</td>
      |  </tr>
      |  <tr>
      |    <td>proc_id</td>
      |    <td>${procId}</td>
      |  </tr>
      |  <tr>
      |    <td>proc_execution_id</td>
      |    <td>${procExecutionId}</td>
      |  </tr>
      |  <tr>
      |    <td>Error details</td>
      |    <td>${errorDetails}</td>
      |  </tr>
      |
      |    <tr>
      |    <td>Error stack trace</td>
      |    <td>${errorStackTrace}</td>
      |  </tr>
      |
      |</table>
      |
      |</body>
      |</html>
      |""".stripMargin
    )
    try {
      sendMail(config ++ jobSpecs)
      FileUtils.deleteDirectory(tmpDir)
    } catch {
      case e : Exception =>
      FileUtils.deleteDirectory(tmpDir)
      throw e
    }
  }

}
