package com.cloud.bigtable;

import java.io.IOException;

/**
 * Created by Christopher Leuer
 * CrawlCorpus as an interface
 */
public interface CrawlCorpus {
    public void createRow(CrawlCorpusRow crawlCorpusRow);
    public CrawlCorpusRow getRow (String rowId);
    public void deleteRow(String rowId);
    public String getTableName();
    public void done();

}
