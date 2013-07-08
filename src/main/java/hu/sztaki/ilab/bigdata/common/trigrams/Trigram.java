/**
 * Trigram.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.trigrams;

/**
 *
 * @author garzo
 */
public class Trigram {

    private final String first;
    private final String second;
    private final String third;

    public Trigram(String s1, String s2, String s3) {
        this.first = s1;
        this.second = s2;
        this.third = s3;
    }

    public String getFirst() {
        return this.first;
    }

    public String getSecond() {
        return this.second;
    }

    public String getThird() {
        return this.third;
    }

}
