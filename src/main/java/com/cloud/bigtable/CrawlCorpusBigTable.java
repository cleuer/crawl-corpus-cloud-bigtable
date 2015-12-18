/**
Created by Christopher Leuer.
 It implements an interface I created named crawlCorpus, so the BigTable implementation can abstracted

Google examples primarily use the Cloud DataFlow connector can involve service charges. So I use the HBase
 client APIs to perform read/writes on the Cloud Bigtable.

A few useful references for the Hbase API:
 https://github.com/GoogleCloudPlatform/cloud-bigtable-examples: simple-cli uses hbase.client
 http://hbase.apache.org/0.94/book/versions.html/
 https://autofei.wordpress.com/2012/04/02/java-example-code-using-hbase-data-model-operations/

 */
package com.cloud.bigtable;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CrawlCorpusBigTable implements CrawlCorpus {

    Connection connection;
    static String tableName = "crawl-corpus-tab";
    static String cfUrl = "url";
    static String cfContent = "content";
    static String cellQualifier = "a";  //  used to reference each cell within a columnFamily
    static Logger log = Logger.getLogger(CrawlCorpusBigTable.class.getName());

   public CrawlCorpusBigTable()  {
       connectGoogleBigTable();
       createTable();
       log.info("CrawlCorpusBigTable initialized");
   }

    @Override
    public void done() {
        try {
            connection.close();
            log.info("connection closed");
        } catch (IOException e) {
            System.exit(0);
        }
    }

    /*
    *  Create a row in the corpus-crawl table
    *  2 columnFamily: url and entity
    */
    @Override
    public void createRow(CrawlCorpusRow crawlCorpusRow) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            // create 2 put requests
            Put putUrl = new Put(Bytes.toBytes(crawlCorpusRow.getRowId()));
            Put putContent = new Put(Bytes.toBytes(crawlCorpusRow.getRowId()));

            // add column to put requests    columns family, cell, value
            putUrl.addColumn(Bytes.toBytes(cfUrl), Bytes.toBytes(cellQualifier), Bytes.toBytes(crawlCorpusRow.getUrl()));
            putContent.addColumn(Bytes.toBytes(cfContent), Bytes.toBytes(cellQualifier), Bytes.toBytes(crawlCorpusRow.getContent()));

            //atomic put for each cell in row
            table.put(putUrl);
            table.put(putContent);
            log.info("row "+ crawlCorpusRow.getRowId() +" created");

        } catch (IOException e) {
            System.out.println("IO error createRow() failed for rowId " + crawlCorpusRow.getRowId());
            e.printStackTrace();
            System.exit(0);
        }
    }

    /*
    *  Returns row with most current version from corpus-crawl-tab using Hbase Gets
    *  row results  is content + url
    */
    @Override
    public CrawlCorpusRow getRow (String rowId) {
        String url = "";
        String content = "";
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowId));
            Result r = table.get(get);
            if (!r.isEmpty()) {
                url = new String(r.getValue(Bytes.toBytes(cfUrl), Bytes.toBytes(cellQualifier)));
                content = new String(r.getValue(Bytes.toBytes(cfContent), Bytes.toBytes(cellQualifier)));
            } else {
                log.info("row " + rowId + " not found");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        log.info("row " + rowId + " found: " + content + " " + url);
        return (new CrawlCorpusRow(rowId,url,content));
    }

    /**
     * Delete a row
     *  Per Apache hbase docs the data does get deleted. Instead a "tombstone" marker is written to each cell to
     *  to mask and hide the data
     *  used example from https://autofei.wordpress.com/2012/04/02/java-example-code-using-hbase-data-model-operations/
     */
    @Override
    public void deleteRow(String rowId) {
        try {
             Table table = connection.getTable(TableName.valueOf(tableName));
             List<Delete> list = new ArrayList<Delete>();
             Delete del = new Delete(rowId.getBytes());
             if (del.isEmpty()) {
                 list.add(del);
                 table.delete(list);
                 log.info("row " + rowId+ " deleted");
             } else {
                 log.info("delete failed for row " + rowId);
             }

        } catch (IOException e) {
            System.out.println("IO error. deleteRow() failed for rowId " + rowId);
            e.printStackTrace();
            System.exit(0);
        }
    }

    @Override
    public String getTableName() {
        return tableName;
    }
    /*
    * connect Google to cloud bigTable
    */
    private void connectGoogleBigTable() {
        try {
            connection = ConnectionFactory.createConnection();
            log.info("connection: " + connection.toString());

        } catch (IOException e) {
            System.out.println("Connection to Google Bigtable failed");
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void createTable() {
        try {
            Admin admin = connection.getAdmin();

            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
                table.addFamily(new HColumnDescriptor(cfUrl));
                table.addFamily(new HColumnDescriptor(cfContent));
                admin.createTable(table);
                log.info("Table created: "+ table.getTableName());
            }
        } catch (IOException e) {
            System.out.println("Table not created. Connection failed");
            e.printStackTrace();
            System.exit(0);
        }
    }

    /* Main used for Unit testing */
    public static void main(String[] args)  {
        CrawlCorpus cw = new CrawlCorpusBigTable();

        //cw.createRow (new CrawlCorpusRow("1","http:/www.google.com","<html>google</html>"));
        //CrawlCorpusRow cwRow1 = cw.getRow("1"); //expect: row 1 found: http:/www.google.com <html>test</html>
        //CrawlCorpusRow cwRow2 = cw.getRow("2"); //expect: row 1 not found
        //cw.deleteRow("1");  //expect: row 1 deleted
        //cw.createRow (new CrawlCorpusRow("3","http:/www.yahoo.com","<html>yahoo</html>")); //expect only row 3 to scan
        CrawlCorpusRow cwRow1 = cw.getRow("158");
        cw.done(); //expect: connection closes
    }
}
