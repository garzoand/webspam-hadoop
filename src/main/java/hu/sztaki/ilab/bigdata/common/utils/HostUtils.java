package hu.sztaki.ilab.bigdata.common.utils;

import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 * @author garzo
 */
public class HostUtils {
    
    public static String determineHostName(String urlText) throws MalformedURLException {
        URL url = new URL(urlText);
        return url.getHost();
    }
    
    public static String determinePath(String urlText) throws MalformedURLException {
        URL url = new URL(urlText);
        return url.getPath();
    }
    
}
