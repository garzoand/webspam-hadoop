package hu.sztaki.ilab.bigdata.common.utils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;


/**
 * Comparator for comparing two Lists
 * Needed for sorting elements of data structures like list of lists:
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 * @param <T> - should be comparable
 */
public class ListComparator<T extends Comparable<? super T>>implements Comparator<List<T>>,
        Serializable {

    private static final long serialVersionUID = 8360233486829474095L;

    @Override
    public int compare(List<T> o1, List<T> o2) {

        for (int i = 0; i < o1.size(); i++) {
            if (o2.size() > i) {
                T t1 = o1.get(i);
                T t2 = o2.get(i);
                int compared = t1.compareTo(t2);
                if (compared != 0) {
                    return compared;
                }
            }
            else {
                return 1;
            }
        }
        return (o2.size() > o1.size()) ? -1 : 0;

    }

}