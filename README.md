# Google Cloud Bigtable S3 Common Crawl ReadMe

Christopher 12/15/2015

Java source file are included in src\main\java\com\cloud

src\main\java\com\cloud
- CrawlCorpus - Interface to abstract storage object for crawl-corpus in Google Cloud
- CrawlCorpusBigTable - Primary class used for crawl-corpus programs include getRow, createRow, deleteRow 
- CrawlCorpusRow - Represent a row in the crawl-corpus
- CrawlCorpusCheck -Class to allow comparison between html from warc and html stored in Google Bigtable
- CrawlCorpusRun - This is the class with main to read the common corpus from Amazon s3 and call methods to write to table
- WarcHTMLResponse, WarcHTMLResponse - unchanged example files from  https://github.com/commoncrawl/example-warc-java to support Warc reading
* See manifest table in E90_LeuerChristopherFinalProjectFull.docx for more details

- src\main\resources\hbase-site.xml is for HBase configuration
- pom.xml is for Maven dependencies
- data\sample.warc - is a sample warc file

\doc
- E90_LeuerChristopher_FinalProjectSlides.ppt - Powerpoint slides
- E90_LeuerChristopherFinalProjectFull.pdf - Summary report
- E90_LeuerChristopherFinalProjectFull.pdf - Full report

Setup instructions are provided in E90_LeuerChristopherFinalProjectFull.docx
