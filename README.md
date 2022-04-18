# MySQL Connector/J

[![GitHub top language](https://img.shields.io/github/languages/top/mysql/mysql-connector-j.svg?color=5382a1)](https://github.com/mysql/mysql-connector-j/tree/release/8.0/src) [![License: GPLv2 with FOSS exception](https://img.shields.io/badge/license-GPLv2_with_FOSS_exception-c30014.svg)](LICENSE) [![Maven Central](https://img.shields.io/maven-central/v/mysql/mysql-connector-java.svg)](https://search.maven.org/artifact/mysql/mysql-connector-java/8.0.30/jar)

MySQL provides connectivity for client applications developed in the Java programming language with MySQL Connector/J, a driver that implements the [Java Database Connectivity (JDBC) API](https://www.oracle.com/technetwork/java/javase/jdbc/) and also [MySQL X DevAPI](https://dev.mysql.com/doc/x-devapi-userguide/en/).

MySQL Connector/J 8.0 is a JDBC Type 4 driver that is compatible with the [JDBC 4.2](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) specification. The Type 4 designation means that the driver is a pure Java implementation of the MySQL protocol and does not rely on the MySQL client libraries.

The driver also contains an implementation of [MySQL X DevAPI](https://dev.mysql.com/doc/x-devapi-userguide/en/), an application programming interface for working with [MySQL as a Document Store](https://dev.mysql.com/doc/refman/8.0/en/document-store.html) through CRUD-based, NoSQL operations.

For more information, please visit the official [MySQL Connector/J documentation](https://dev.mysql.com/doc/connector-j/8.0/en/).

## Licensing

Please refer to the [README](README) and [LICENSE](LICENSE) files, available in this repository, and the [Legal Notices in the Connector/J documentation](https://dev.mysql.com/doc/connector-j/8.0/en/preface.html) for further details.

## Getting the Latest Release

MySQL Connector/J is free for usage under the terms of the specified licensing and it runs on any operating system that is able to run a Java Virtual Machine.

### Download and Install

MySQL Connector/J can be installed from pre-compiled packages that can be downloaded from the [Connector/J download page](https://dev.mysql.com/downloads/connector/j/). Installing Connector/J only requires obtaining the corresponding JAR file from the downloaded bundle or installer and including it in the application's CLASSPATH.

According to how you use Connector/J, you may also need to install the following third-party libraries on your system for it to work:
* Protocol Buffers (protobuf-java) is required for using X DevAPI
* Oracle Cloud Infrastructure SDK for Java (oci-java-sdk) is required to support OCI AIM authentication
* Simple Logging Facade API (slf4j-api) is required for using the logging capabilities provided by the default implementation of org.slf4j.Logger.Slf4JLogger by Connector/J 

### As a Maven Dependency

Alternatively, Connector/J can be obtained automatically via [Maven's dependency management](https://search.maven.org/search?q=g:mysql%20AND%20a:mysql-connector-java) by adding the following configuration in the application's Project Object Model (POM) file:

```xml
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>8.0.30</version>
</dependency>
```

Connector/J's own Project Object Model (POM) file specifies a transitive dependency to Protocol Buffers (protobuf-java) since it is required for using X DevAPI. However, if you do not use the X DevAPI features, you may also want to add a dependency exclusion to avoid linking the unneeded sub-library. For example:

```xml
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>8.0.30</version>
  <exclusions>
    <exclusion>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
    </exclusion>
  </exclusions> 
</dependency>
```

### Build From Source

This driver can also be complied and installed from the source available in this repository. Please refer to the Connector/J documentation for [detailed instructions](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-installing-source.html) on how to do it.

### GitHub Repository

This repository contains the MySQL Connector/J source code as per the latest release. No changes are made in this repository between releases.

## Contributing

There are a few ways to contribute to the Connector/J code. Please refer to the [contributing guidelines](CONTRIBUTING.md) for additional information.

## Additional Resources

* [MySQL Connector/J Developer Guide](https://dev.mysql.com/doc/connector-j/8.0/en/).
* [MySQL Connector/J X DevAPI Reference](https://dev.mysql.com/doc/dev/connector-j/8.0/).
* [MySQL Connector/J, JDBC and Java forum](https://forums.mysql.com/list.php?39).
* [`#connectors` channel in MySQL Community Slack](https://mysqlcommunity.slack.com/messages/connectors). ([Sign-up](https://lefred.be/mysql-community-on-slack/) required if you do not have an Oracle account.)
* [@MySQL on Twitter](https://twitter.com/MySQL).
* [MySQL Blog](https://blogs.oracle.com/mysql/).
* [MySQL Connectors Blog archive](https://dev.mysql.com/blog-archive/?cat=Connectors%20%2F%20Languages).
* [MySQL Newsletter](https://www.mysql.com/news-and-events/newsletter/).
* [MySQL Bugs Database](https://bugs.mysql.com/).

For more information about this and other MySQL products, please visit [MySQL Contact & Questions](https://www.mysql.com/about/contact/).

[![Twitter Follow](https://img.shields.io/twitter/follow/MySQL.svg?label=Follow%20%40MySQL&style=social)](https://twitter.com/intent/follow?screen_name=MySQL)
