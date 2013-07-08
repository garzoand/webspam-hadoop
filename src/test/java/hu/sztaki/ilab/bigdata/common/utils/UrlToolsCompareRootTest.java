package hu.sztaki.ilab.bigdata.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class UrlToolsCompareRootTest {

    private List<String> urlList = new ArrayList<String>();
    private String expectedUrl = null;
    
    @Before
    public void init() {
        
        urlList.add("http://www.stumbleupon.com/atom/defaul.php");
        urlList.add("http://www.stumbleupon.com/test/d/s/a/index.html");
        urlList.add("http://www.stumbleupon.com");
        urlList.add("http://www.stumbleupon.com/test/none.css");
        urlList.add("http://www.stumbleupon.com/");
        urlList.add("http://www.stumbleupon.com/a/b/my.html");
        
        expectedUrl = "http://www.stumbleupon.com";
    }
    
    @Test
    public void comparatorTest() {
        String rootUrl = Collections.max(urlList, UrlUtils.HOMEPAGE_URL_COMPARATOR);
        Assert.assertEquals(expectedUrl, rootUrl);
        
    }
    
}
