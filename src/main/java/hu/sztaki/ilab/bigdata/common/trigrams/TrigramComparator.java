/**
 * TrigramComparator.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.trigrams;

import java.util.Comparator;

/**
 *
 * @author garzo
 */
public class TrigramComparator implements Comparator {

    public int compare(Object x, Object y) {
        Trigram t1 = (Trigram)x;
        Trigram t2 = (Trigram)y;

        if (t1.getFirst().equals(t2.getFirst())) {
            if (t1.getSecond().equals(t2.getSecond())) {
                return t1.getThird().compareTo(t2.getThird());
            } else {
                return t1.getSecond().compareTo(t2.getSecond());
            }
        } else {
            return t1.getFirst().compareTo(t2.getFirst());
        }        
    }
}
