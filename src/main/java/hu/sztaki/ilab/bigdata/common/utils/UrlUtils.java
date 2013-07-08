package hu.sztaki.ilab.bigdata.common.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * Utility class for business specific URL processing
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 * 
 */
public class UrlUtils {

    private static final Pattern INDEX_PATTERN = Pattern.compile("^/((?:index|default)\\.\\w+)$");
    
    /**
     * Comparator to select the 'root url'. A url is greater if it is:
     * - a domain url (E.g: www.example.com, www.example.com/) 
     * - ends with index.*
     * - has less paths
     */
    public static final Comparator<String> HOMEPAGE_URL_COMPARATOR = new Comparator<String>() {
        public int compare(String s1, String s2) {
            return UrlUtils.compareRootUrls(s1, s2);
        }
    };
    
    private UrlUtils() {
        throw new AssertionError("shouldn't be instantiated");
    }

    /**
     * Returns ip as a long value
     * 
     * @param addr
     * @return
     */
    public static long ipAsLong(String addr) {
        String[] addrArray = addr.split("\\.");
        long num = 0;
        for (int i = 0; i < addrArray.length; i++) {
            int power = 3 - i;
            num += ((Integer.parseInt(addrArray[i]) % 256 * Math.pow(256, power)));
        }
        return num;
    }

    /**
     * Converts long ip to a String representation
     * 
     * @param val
     * @return
     */
    public static String ipAsString(long val) {
        StringBuilder sb = new StringBuilder();
        sb.append((val >> 24) & 0xFF)
          .append(".")
          .append(((val >> 16) & 0xFF))
          .append(".")
          .append(((val >> 8) & 0xFF))
          .append(".")
          .append((val & 0xFF));
          
        return sb.toString();
    }
    
    /**
     * Returns the host part from the url
     * 
     * @param url - String url to parse
     * @return
     */
    public static String getHostFromUrl(String url) {

        String result = null;
        if (StringUtils.isEmpty(url)) {
            return result;
        }

        int doubleslash = url.indexOf("//");
        doubleslash = (doubleslash == -1) ? 0 : doubleslash + 2;
        int end = url.indexOf('/', doubleslash);
        end = (end >= 0) ? end : url.length();

        return result = url.substring(doubleslash, end);
    }

    /**
     * Returns path part of the url
     * 
     * @param url
     * @return
     */
    public static String getPathFromFromUrl(String url) {

        String result = null;
        if (StringUtils.isEmpty(url)) {
            return result;
        }

        int doubleslash = url.indexOf("//");
        doubleslash = (doubleslash == -1) ? 0 : doubleslash + 2;

        int end = url.indexOf('/', doubleslash);
        end = (end >= 0) ? end : url.length();

        return result = url.substring(end, url.length());
    }
    
    /**
     * Compares two urls based on their 'distance' from the root url.
     * An url is considered to be greater than the other if it is more likely
     * a root url (e.g: http://www.foo.com/)
     * 
     * @param url1
     * @param url2
     * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second
     */
    public static int compareRootUrls(String url1, String url2) {

        int result = 0;

        // first normalize url
        URI u1 = null;
        URI u2 = null;
        try {

            u1 = new URI(url1).normalize();
            u2 = new URI(url2).normalize();

        } catch (URISyntaxException e) {
            if (u1 == null && u2 == null) {
                return 1; // mustn't happen
            }
            else {
                return (u1 == null) ? -1 : 1;
            }
        }

        String s1Path = UrlUtils.getPathFromFromUrl(u1.toString());
        String s2Path = UrlUtils.getPathFromFromUrl(u2.toString());

        result = compareRoot(s1Path, s2Path);
        if (result != 0) {
            return result;
        }
        result = compareIndex(s1Path, s2Path);
        if (result != 0) {
            return result;
        }

        result = comparePathDepth(s1Path, s2Path);
        if (result == 0) {
            return 1; //just pick one..
        }
        else {
            return result;
        }
    }

    private static int compareRoot(String s1Path, String s2Path) {
        return isRoot(s1Path).compareTo(isRoot(s2Path));
    }

    private static int compareIndex(String s1Path, String s2Path) {

        boolean s1Index = INDEX_PATTERN.matcher(s1Path).matches();
        boolean s2Index = INDEX_PATTERN.matcher(s2Path).matches();
        return (s1Index == s2Index) ? 0 : (s1Index ? 1 : -1);

    }

    private static int comparePathDepth(String s1Path, String s2Path) {
        int s1PathNum = StringUtils.countMatches(s1Path, "/");
        int s2PathNum = StringUtils.countMatches(s2Path, "/");
        return (s1PathNum > s2PathNum) ? -1 : ((s1PathNum == s2PathNum) ? 0 : 1);
    }

    private static Boolean isRoot(String url) {
        return (StringUtils.isEmpty(url) || "/".equals(url));
    }

}
