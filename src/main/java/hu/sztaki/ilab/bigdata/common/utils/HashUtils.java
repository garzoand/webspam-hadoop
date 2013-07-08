package hu.sztaki.ilab.bigdata.common.utils;

/**
 *
 * @author garzo
 */
public class HashUtils {
    
    // suitable for strings with length >= 5
    public static long hashCode64(String string) {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = 31*h + string.charAt(i);
        }
        return h;
    }
}
