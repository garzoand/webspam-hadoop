package hu.sztaki.ilab.bigdata.common.record;

import hu.sztaki.ilab.bigdata.common.testutils.SerializationUtils;
import hu.sztaki.ilab.bigdata.common.utils.UrlUtils;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class HomePageMetaDataTest {

    private HomePageMetaData data1 = null;
    private HomePageMetaData data2 = null;
    private HomePageMetaData data3 = null;

    @Before
    public void init() {

        String url = "http://example.com/index.html";
        String hostName = "example.com";
        long ip = UrlUtils.ipAsLong("192.168.0.1");
        String path = "/";
        long collectionId = 1234l;
        long timestamp = System.currentTimeMillis();

        data1 = new HomePageMetaData.Builder(url, hostName)
                .collectionID(collectionId)
                .ip(ip)
                .path(path)
                .timestamp(timestamp)
                .build();
        
        data2 = new HomePageMetaData.Builder(url, hostName).build();
        
        data3 = new HomePageMetaData.Builder(url, hostName)
                .ip(ip)
                .timestamp(timestamp)
                .build();
        
    }

    @Test
    public void serializationRoundTripTest() throws IOException {
        
        doTest(data1);
        doTest(data2);
        doTest(data3);
        
    }
    
    private void doTest(HomePageMetaData data) throws IOException {
        
        //serialize data
        byte[] serializedBytes = SerializationUtils.serialize(data);
        
        //deserialize data
        HomePageMetaData deserialized = new HomePageMetaData.Builder("", "").build();
        SerializationUtils.deserialize(deserialized, serializedBytes);

        //compare
        Assert.assertEquals("Url mismatch!", getEmptyIfNull(data.getUrl()),
                getEmptyIfNull(deserialized.getUrl()));

        Assert.assertEquals("Hostname mismatch!", getEmptyIfNull(data.getHostName()),
                getEmptyIfNull(deserialized.getHostName()));

        Assert.assertEquals("Path mismatch", getEmptyIfNull(data.getPath()),
                getEmptyIfNull(deserialized.getPath()));

        Assert.assertEquals("IP mismatch", data.getIp(), deserialized.getIp());

        Assert.assertEquals("CollectionID mismatch", data.getCollectionID(),
                deserialized.getCollectionID());

        Assert.assertEquals("Timestamp mismatch", data.getTimestamp(), deserialized.getTimestamp());
        
    }
   
    private String getEmptyIfNull(String val) {
        return (val == null || "null".equals(val)) ? "" : val;
    }

}
