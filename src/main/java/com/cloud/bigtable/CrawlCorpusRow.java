/**
 * Created by Christopher Leuer
 * CrawlCorpusRow as a Java bean , oh the verbosity
 */

package com.cloud.bigtable;

public class CrawlCorpusRow {
    private String rowId;
    private String url;
    private String content;

    public CrawlCorpusRow (String rowId, String url, String content) {
        this.rowId = rowId;
        this.url = url;
        this.content = content;
    }

    public void setRowId (String rowId) {
        this.rowId = rowId;
    }

    public String getRowId () {
        return rowId;
    }

    public void setUrl (String url) {
        this.url = url;
    }

    public String getUrl () {
        return url;
    }

    public void setContent (String content) {
        this.content = content;
    }

    public String getContent () {
        return content;
    }
}
