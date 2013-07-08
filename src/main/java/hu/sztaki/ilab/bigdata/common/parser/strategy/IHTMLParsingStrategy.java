/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.common.parser.strategy;

import hu.sztaki.ilab.bigdata.common.parser.ParseResult;

/**
 *
 * @author garzo
 */
public interface IHTMLParsingStrategy {

    void parseHTML(String html, ParseResult result);

}
