/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.spam.wordcount.query.post;

import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

/**
 *
 * @author garzo
 */
public interface IQueryWordCountPostProcessStrategy {

    public void postProcess(DictionaryTrie trie);

}
