package hu.sztaki.ilab.bigdata.common.input.readers;

import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 * @author garzo
 */
public interface IReader {
    
    public HomePageContentRecord readNextRecord(DataInputStream steam)
            throws IOException;
    public long getTotalRecordLength();
    
}
