# Google Cloud Bigtable S3 Common Crawl ReadMe

Christopher 12/15/2015

Read web pages from public common crawl corpus stored in Amazon s3 into Google Cloud Big Table. 

To include REST

Java source file are included in src\main\java\com\cloud

- CrawlCorpus Interface to abstract storage object for crawl-corpus in Google Cloud
- CrawlCorpusBigTablePrim ary class used for crawl-corpus programs include getRow, createRow, deleteRow 
- CrawlCorpusRow Represent a row in the crawl-corpus
- CrawlCorpusCheck Class to allow comparison between html from warc and html stored in Google Bigtable
- CrawlCorpusRun This is the class with main to read the common corpus from Amazon s3 and call methods to write to table
- WarcHTMLResponse, WarcHTMLResponse - unchanged example files from  https://github.com/commoncrawl/example-warc-java
* See manifest table in E90_LeuerChristopherFinalProjectFull.docx for more details

- src\main\resources\hbase-site.xml is for HBase configuration
- pom.xml is for Maven dependencies
- data\sample.warc - is a sample warc file

sample setting.xml in $HOME/.m2 directory will automatically download depedenciess

    <?xml version="1.0" encoding="UTF-8"?>
    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">

    <pluginGroups>
    <pluginGroup>org.jboss.maven.plugins</pluginGroup>
    </pluginGroups>

    <profiles>
    <profile>

    <repositories>
        <repository>
            <id>my-alternate-repository</id>
            <url>http://central.maven.org/maven2</url>
        </repository>

        <repository>
            <id>codehausSnapshots</id>
            <name>Codehaus Snapshots</name>
            <releases>
                <enabled>false</enabled>
                <updatePolicy>always</updatePolicy>
                <checksumPolicy>warn</checksumPolicy>
            </releases>
            <snapshots>
                <enabled>true</enabled>
                <updatePolicy>never</updatePolicy>
                <checksumPolicy>fail</checksumPolicy>
            </snapshots>
            <url>http://snapshots.maven.codehaus.org/maven2</url>
            <layout>default</layout>
        </repository>

    </repositories>

    </profile>
    </profiles>

    </settings>
