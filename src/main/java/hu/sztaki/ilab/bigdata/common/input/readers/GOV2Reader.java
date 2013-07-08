/**
 * GOV2Reader.java
 * Reads a record from a GOV2 data file.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.input.readers;

import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.utils.StreamUtils;
import hu.sztaki.ilab.bigdata.common.utils.UrlUtils;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;

/**
 *
 * @author garzo
 */
public class GOV2Reader implements IReader {

    private static final String RECORD_BEGIN = "<DOC>";
    private static final String RECORD_END = "</DOC>";
    private static final String HEADER_BEGIN = "<DOCHDR>";
    private static final String HEADER_END = "</DOCHDR>";
    private static final String DOCNO = "<DOCNO>"; 
    private static final String DOCNO_END = "</DOCNO>"; 
    private static final String NEW_LINE = "\n";
    public static final String GOV2ID_PROPNAME = "GOV2ID";
    
    private long totalRead = 0;
    private long recordRead = 0;
    
    
    @Override
    public HomePageContentRecord readNextRecord(DataInputStream steam) throws IOException {
        
        String line = null;
        String url = null;
        String GOV2ID = "";
        long collectionID = 0;
        boolean foundMark = false;
        StringBuilder builder = new StringBuilder();
        this.totalRead = 0;
        
        // record begin
        while (!foundMark && (line = StreamUtils.nonBufferedReadLine(steam)) != null) {
            if (line.startsWith(RECORD_BEGIN)) {
                foundMark = true;
            }
        }
        if (!foundMark) {
            return null;
        }
        
        // header
        foundMark = false;
        while (!foundMark && (line = StreamUtils.nonBufferedReadLine(steam)) != null) {
            if (line.startsWith(DOCNO)) {
                // GOV2ID = Long.parseLong(line.replaceAll("[^\\d]", ""));                
                GOV2ID = line.replaceAll(DOCNO, "").replaceAll(DOCNO_END, "");
            }
            if (line.startsWith(HEADER_BEGIN)) {
                foundMark = true;
            }
        }
        if (!foundMark) {
            return null;
        }
        url = StreamUtils.nonBufferedReadLine(steam);
        if (url == null) {
            return null;
        }
        foundMark = false;
        while (!foundMark && (line = StreamUtils.nonBufferedReadLine(steam)) != null) {
            if (line.startsWith(HEADER_END)) {
                foundMark = true;
            } else {
                builder.append(line).append(NEW_LINE);
                this.totalRead += line.length();
            }
        }
        if (!foundMark) {
            return null;
        }
        
        // content
        foundMark = false;
        while (!foundMark && (line = StreamUtils.nonBufferedReadLine(steam)) != null) {
            if (line.startsWith(RECORD_END)) {
                foundMark = true;
            } else {
                builder.append(line).append(NEW_LINE);
                this.totalRead += line.length();
            }
        }
        if (!foundMark) {
            return null;
        }
        
        // determine collection ID
        // GX256-00-0836653
        //   25600 00000
        String[] fields = GOV2ID.split("-");
        if (fields.length < 3) {
            collectionID = 0;
        } else {
            collectionID = Long.parseLong(fields[0].replaceAll("GX", "")) * 10000000;
            collectionID += Long.parseLong(fields[1]) * 100000;
            collectionID += recordRead;
        }
        
        HomePageMetaData meta = new HomePageMetaData.Builder(url, UrlUtils.getHostFromUrl(url))
                .path(UrlUtils.getPathFromFromUrl(url))
                .timestamp(0)
                .collectionID(collectionID)
                .addCustomPropery(GOV2ID_PROPNAME, GOV2ID.getBytes())
                .build();
        HomePageContentRecord record = new HomePageContentRecord(
                new BytesWritable(builder.toString().getBytes()),
                meta);
        recordRead++;
        return record;
    }

    @Override
    public long getTotalRecordLength() {
        return this.totalRead;
    }
    
}
