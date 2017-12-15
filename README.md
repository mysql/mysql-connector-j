# MySQL Connector/J

MySQL provides connectivity for client applications developed in the Java programming language with MySQL Connector/J, a driver that implements the [Java Database Connectivity (JDBC) API](http://www.oracle.com/technetwork/java/javase/jdbc/).

MySQL Connector/J 8.0 is a JDBC Type 4 driver that is compatible with the [JDBC 4.2](http://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) specification. The Type 4 designation means that the driver is a pure Java implementation of the MySQL protocol and does not rely on the MySQL client libraries.

For detailed information please visit the official [MySQL Connector/J documentation](http://dev.mysql.com/doc/connector-j/en/).

## Licensing

Please refer to files README and LICENSE, available in this repository, and [Legal Notices in documentation](http://dev.mysql.com/doc/connector-j/en/preface.html) for further details. 

## Download & Install

MySQL Connector/J can be installed from pre-compiled packages that can be downloaded from the [MySQL downloads page](http://dev.mysql.com/downloads/connector/j/). Installing Connector/J only requires extracting the corresponding Jar file from the downloaded bundle and place it somewhere in the application's CLASSPATH.

Alternatively you can setup [Maven's dependency management](http://search.maven.org/#search|ga|1|g%3A%22mysql%22%20AND%20a%3A%22mysql-connector-java%22) directly in your project and let it download it for you.

### Building from sources

This driver can also be complied and installed from the sources available in this repository. Please refer to the documentation for [detailed instructions](http://dev.mysql.com/doc/connector-j/en/connector-j-installing-source.html) on how to do it.

### GitHub Repository

This repository contains the MySQL Connector/J source code as per latest released version. You should expect to see the same contents here and within the latest released Connector/J package, except for the pre-compiled Jar.

## Additional resources

- [MySQL Connector/JDBC and Java forum](http://forums.mysql.com/list.php?39)
- [MySQL and Java Mailing Lists](http://lists.mysql.com/java)
- [InsideMySQL Connectors Blog](http://insidemysql.com/category/mysql-development/connectors/)
- [MySQL Connectors Java Blog](https://blogs.oracle.com/mysqlconnectors-java)
- [MySQL Bugs database](http://bugs.mysql.com/)
- For more information about this and other MySQL products, please visit [MySQL Contact & Questions](http://www.mysql.com/about/contact/).

