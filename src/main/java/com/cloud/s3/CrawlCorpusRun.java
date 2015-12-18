/** This file was created from an example on  https://github.com/commoncrawl/example-warc-java
 *  The original author is Mark Watson.  Very little was changed, except it uses a new callback class
 *  to initialize CrawlCorpusBigTable, and added a counter named warcRecordId
*/
package com.cloud.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.cloud.bigtable.CrawlCorpus;
import com.cloud.bigtable.CrawlCorpusBigTable;
import com.cloud.bigtable.CrawlCorpusRow;
import org.apache.log4j.Logger;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class CrawlCorpusRun {
    Logger log = Logger.getLogger(CrawlCorpusBigTable.class.getName());

    public void process(AmazonS3 s3, String bucketName, String prefix, int max, CrawlCorpus crawlCorpus) {
        int count = 0;
        ObjectListing list = s3.listObjects(bucketName, prefix);

        do {  // reading summaries code derived from stackoverflow example posted by Alberto A. Medina:
            int warcRecordId = 1;
            List<S3ObjectSummary> summaries = list.getObjectSummaries();
            for (S3ObjectSummary summary : summaries) {
                try {
                    String key = summary.getKey();
                    System.out.println("+ key: " + key);
                    S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
                    InputStream objectData = object.getObjectContent();
                    GZIPInputStream gzInputStream=new GZIPInputStream(objectData);
                    DataInputStream inStream = new DataInputStream(gzInputStream);

                    WarcRecord thisWarcRecord;
                    while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
                        //System.out.println("-- thisWarcRecord.getHeaderRecordType() = " + thisWarcRecord.getHeaderRecordType());
                        if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                            WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(thisWarcRecord);
                            String thisTargetURI = htmlRecord.getTargetURI();
                            String thisContentUtf8 = htmlRecord.getRawRecord().getContentUTF8();
                            // handle WARC record content:
                            log.info("warcUrl: "+ thisTargetURI);
                            crawlCorpus.createRow (new CrawlCorpusRow(Integer.toString(warcRecordId), thisTargetURI, thisContentUtf8));
                            warcRecordId++;
                        }
                    }
                    inStream.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                if (++count >= max) return;
            }
            list = s3.listNextBatchOfObjects(list);
        } while (list.isTruncated());
        // done processing all WARC records:
        crawlCorpus.done();
    }

    static public void main(String[] args) {
        AmazonS3Client s3 = new AmazonS3Client();
        // use a callback class for handling WARC record data:
        CrawlCorpus crawlCorpus = new CrawlCorpusBigTable();
        CrawlCorpusRun run = new CrawlCorpusRun();
        run.process(s3, "aws-publicdatasets", "common-crawl/crawl-data/CC-MAIN-2013-48", 2, crawlCorpus);
    }
}
