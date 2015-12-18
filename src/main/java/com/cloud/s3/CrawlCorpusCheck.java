/**
 * Created by Christopher Leuer
 * The ReadWarc method is modified version of the main from
 * https://github.com/commoncrawl/example-warc-java/blob/master/src/main/java/org/commoncrawl/examples/java_warc/ReadWARC.java
 */

package com.cloud.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import com.cloud.bigtable.CrawlCorpus;
import com.cloud.bigtable.CrawlCorpusBigTable;
import com.cloud.bigtable.CrawlCorpusRow;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;


public class CrawlCorpusCheck {

    protected WarcHTMLResponseRecord warcHTMLResponseRecord;
    protected CrawlCorpus crawlCorpus;
    protected CrawlCorpusRow crawlCorpusRow;
    static Logger log = Logger.getLogger(CrawlCorpusBigTable.class.getName());

    public CrawlCorpusCheck (AmazonS3 s3, String bucketName, String warcFile, String warcUrl, String RowId) throws IOException {
        findWarc(s3,bucketName, warcFile, warcUrl);
        this.crawlCorpus = new CrawlCorpusBigTable();
        this.crawlCorpusRow = crawlCorpus.getRow(RowId);
    }

     /*
     * return true if html content matches between warc and corpus crawl row
     */

    public boolean contentIsEqual () {
        Integer wHash = warcHTMLResponseRecord.getRawRecord().getContentUTF8().hashCode();
        Integer cHash = crawlCorpusRow.getContent().hashCode();

        log.info("Warc HTML hash is "+ wHash);
        log.info("crawlCorpus HTML hash is "+ cHash);

        if (cHash.equals(wHash)) {
            return true;
        } else {
            return false;
        }
    }

    //
    // based on an example from http://boston.lti.cs.cmu.edu/clueweb09/wiki/tiki-index.php?page=Working+with+WARC+Files
    public void findWarc(AmazonS3 s3, String bucketName, String warcFile, String warcUrl) throws IOException {

        S3Object object = s3.getObject(new GetObjectRequest(bucketName, warcFile));
        InputStream objectData = object.getObjectContent();
        GZIPInputStream gzInputStream=new GZIPInputStream(objectData);
        DataInputStream inStream=new DataInputStream(gzInputStream);

        WarcRecord thisWarcRecord;
        while ((thisWarcRecord= WarcRecord.readNextWarcRecord(inStream))!=null) {
            System.out.println("%% thisWarcRecord.getHeaderRecordType() = " + thisWarcRecord.getHeaderRecordType());
            if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                WarcHTMLResponseRecord htmlRecord =new WarcHTMLResponseRecord(thisWarcRecord);
                String thisTargetURI= htmlRecord.getTargetURI();
                String thisContentUtf8 = htmlRecord.getRawRecord().getContentUTF8();
                  if (warcUrl.equals(thisTargetURI)) {
                      warcHTMLResponseRecord = htmlRecord;
                      log.info("url found in warc");
                  }
            }
        }
        inStream.close();
        // done processing all WARC records:
    }

    static public void main(String[] args) {

        String bucketName = "aws-publicdatasets";
        String warcFile="common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz";
        String warcUrl = "http://aaamath.com/B/fra35ax2.htm";
        String rowId = "123";

        try {
            AmazonS3Client s3 = new AmazonS3Client();
            CrawlCorpusCheck cc = new CrawlCorpusCheck(s3, bucketName, warcFile, warcUrl, rowId);
            if (cc.contentIsEqual()) {
                System.out.println("Google Cloud Bigtable html for row id " + rowId + " matches Warc for " + warcUrl);
            } else {
                System.out.println("Google Cloud Bigtable html for row id " + rowId + "  does not match Warc for " + warcUrl);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

    }
}
