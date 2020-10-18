[![bkosaraju](https://circleci.com/gh/bkosaraju/utils.svg?style=shield)](https://app.circleci.com/pipelines/github/bkosaraju/utils)
[![codecov](https://codecov.io/gh/bkosaraju/utils/branch/development/graph/badge.svg?token=QHHLFEEHE2)](https://codecov.io/gh/bkosaraju/utils)
[![Apache license](https://img.shields.io/github/issues/bkosaraju/utils.svg
)](https://github.com/bkosaraju/utils/issues)
[![Apache license](https://img.shields.io/crates/l/Apache?color=green&label=Apache%202.0&logo=apache&logoColor=red
)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Version](https://img.shields.io/github/v/tag/bkosaraju/utils?label=Current%20Version
)]()


# Utils

Utils is a library for Application collection of utilities for trigger and track spark Application in

Framework Runners 
-----------------------

this package support running applications in various infrastructurs that include 

1. Kubernetes
2. Databricks
3. EMR 

AWS Helper Utilities
----------------------- 
1. S3
2. SSM
3. EMR 

Common Helper Utilities
-----------------------
1. Mail application
2. Template wrappers
3. String and Map Manipulations
4. Crypto Utilities (encryption and decryption Strings/data in tables)
5. Splunk HEC integration

Spark Utilities
---------------

1. Hash generator
2. CDC Computation
3. Column level Encryption and decryption
4. Dataframe reader (common interface for all apps) and writer

Where can I get the latest release?
-----------------------------------
You can download source from [SCM](https://github.com/bkosaraju/utils).

Alternatively you can pull binaries from the central Maven repositories:
For mvn: 
```xml
<dependency>
  <groupId>io.github.bkosaraju</groupId>
  <artifactId>utils_#ScalaVariant#</artifactId>
  <version>#UtilsVersion#</version>
</dependency>
 
<!--Fat/ Assembly Jar-->
<dependency>
  <groupId>io.github.bkosaraju</groupId>
  <artifactId>utils_#ScalaVariant#</artifactId>
  <version>#UtilsVersion#</version>
  <classifier>all</classifier>
</dependency>

```
for Gradle: 

```groovy
    api group: "io.github.bkosaraju", name: "utils_$scalaVariant", version: "$UtilsVersion"
```

## Build Instructions 

```bash
./gradlew clean build

> Configure project :

> Task :scaladoc
model contains 174 documentable templates

> Task :test

io.github.bkosaraju.utils.Tests

  Test DataHashGenerator: Able to generate hash for given input dataframe PASSED
  Test DataHashGenerator: Throw exception while not be able to generate hash PASSED
  Test DataHashGenerator: generate Hash for SHA512 PASSED
  Test HubLoader: Able to generate hash for given input dataframe PASSED
  Test HubLoader: Able to generate hash for given input dataframe compared with given Key column PASSED
  Test HubLoader: Throw exception incase if source code is unable to produce delta changes PASSED
  Test stringToMap : Test to be able to convert input params String to Map PASSED
  Test stringToMap : Test to be able to convert input params String to Map in case of empty string PASSED
  Test stringToMap : Unable to convert given string into a Map as that was not in valid keyvalue pair format(k1=v1) PASSED
  Test encryptString :  Encrypt String PASSED
  Test decryptString :  Decrypt String PASSED
  Test encryptFile  : Encrypt  File PASSED
  Test decryptFile : Decrypt File PASSED
  Test decrypt : Raise an exception in case if it cant be decrypted PASSED
  Test getKey : Raise an exception in case if it cant generate key PASSED
  Test encryptFile : Raise an exception in case if it cant encrypt File PASSED
  Test decryptFile : Raise an exception in case if it cant decrypt File PASSED
  Test encryptString : Raise an exception in case if it cant encrypt string PASSED
  Test decryptString : Raise an exception in case if it cant decrypt string PASSED
  Test crypt : Raise an exception in case if there is any issue with crypting the stream PASSED
  Test encrypt : Raise an exception in case if it cant be encrypted PASSED
  Test decrypt  : Raise an exception in case if it cant be decrypted PASSED
  Test DataEncryptorTests: Able to encrypt given input columns in dataframe PASSED
  Test DataEncryptorTests: Able to decrypt given input columns in dataframe PASSED
  Test DataEncryptorTests: Able to decrypt given input columns in dataframe(Date DataType) PASSED
  Test DataEncryptorTests: Throw exception in case if not be able te encrypt columns PASSED
  Test DataEncryptorTests: Throw exception in case if not be able te decrypt columns PASSED
  Test DataEncryptorTests: Throw exception in case if not be able te decrypt columns(with out encryption) PASSED
  Test writeData : Write data into Target location PASSED
  Test writeData : Raise an exception in case if there is any issue with writing the data to HDFS PASSED
  Test SendMail : Able to send mail(raise exception for misconfiguration).. PASSED
  Test SendMail : Able to send mail in HTML format(raise exception for misconfiguration).. PASSED
  Test SendMail : send mail from exception.. PASSED
  Test SendMail : Able to send mail(raise exception for misconfiguration non auth).. PASSED
  Test amendDwsCols : Add Audit columns to source dataframe - Function check PASSED
  Test amendDwsCols : Add Audit columns to source dataframe - Function check with empty keys PASSED
  Test amendDwsCols : Add Audit columns to source dataframe - Column count check PASSED
  Test amendDwsCols : Add Audit columns to source dataframe - Return value check PASSED
  Test amendDwsCols : Raise an exception in-case if there is any issue while amending the data warehousing columns PASSED
  Test Context : Context Creation for SQL PASSED
  Test convNonStdDateTimes : Test Non Standard data types being converted to Standard date and time values - check the count PASSED
  Test convNonStdDateTimes : Test Non Standard data types being converted to Standard date and time values - check data PASSED
  Test convNonStdDateTimes : Test Non Standard data types being converted to Standard date and time values - check results PASSED
  Test convNonStdDateTimes : Raise an exception incase if there is any issue with Converting non Standard date times PASSED
  Test getDeltaDF: Exception in case if system is not able to generate Delta from give dataframes. PASSED
  Test srcToTgtColRename : rename the source columns to target columns PASSED
  Test srcToTgtColRename : Unable to rename column and throws exception in case if there is any issue with given input map PASSED
  Test templateWrapper: Raise an exception in case if there is any issue with template building PASSED
  Test templateWrapper: Successfully transform the template PASSED (7.4s)
  Test MaskSensitiveValuesFromMap: mask the sensitive values PASSED
  Test MaskSensitiveValuesFromMap: mask the sensitive values - exclude non-sensitive values PASSED
  Test MaskSensitiveValuesFromMap: mask the sensitive values - Use custom sensitive value list PASSED
  Test MaskSensitiveValuesFromMap: mask the sensitive values - do not mask if vaule is not specified in sensitive value list PASSED
  Test MaskSensitiveValuesFromMap: mask the sensitive values - use default mask list in case of sensitive values not passed PASSED

SUCCESS: Executed 54 tests in 38.6s



#Artifacts can be found in build/lib directory 
```

## Scala [Docs](https://bkosaraju.github.io/utils)

## Contributing
Please feel free to raise a pull request in case if you feel like something can be updated or contributed

## License
[Apache](http://www.apache.org/licenses/LICENSE-2.0.txt)
