/**
 * ICUBasedTokenizingStrategy.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */

package hu.sztaki.ilab.bigdata.common.tokenize.strategy;

import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.TokenType;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang.ArrayUtils;

/**
 *
 * @author garzo
 */
public class ICUBasedTokenizingStrategy implements ITokenizingStrategy {

    private final static Map<Integer, TokenType> RULE_ENTITY_MAP =
            ArrayUtils.toMap(new Object[][]{
                {new Integer(0), TokenType.UNKNOWN},
                {new Integer(100), TokenType.NUMBER},
                {new Integer(200), TokenType.WORD},
                {new Integer(500), TokenType.ABBREVIATION},
                {new Integer(501), TokenType.WORD},
                {new Integer(502), TokenType.INTERNET},
                {new Integer(503), TokenType.INTERNET},
                {new Integer(504), TokenType.MARKUP},
                {new Integer(505), TokenType.EMOTICON},});
    private String text;
    private int index = 0;
    private RuleBasedBreakIterator breakIterator;

    public ICUBasedTokenizingStrategy() {
        this.breakIterator =
                (RuleBasedBreakIterator)BreakIterator.getWordInstance(Locale.getDefault());        
    }

    @Override
    public void setText(String text) {
        if (text == null)
            return;
        this.text = text;
        this.breakIterator.setText(text);
        this.index = 0;
    }

    @Override
    public boolean nextToken(Token token) {
        for (;;) {
            int end = breakIterator.next();
            if (end == BreakIterator.DONE) {
                return false;
            }
            String nextWord = text.substring(index, end);
            index = end;
            TokenType type = RULE_ENTITY_MAP.get(breakIterator.getRuleStatus());
            if (type == null) {
                token.setType(TokenType.UNKNOWN);
            } else {
                token.setType(type);
            }
            token.setValue(nextWord);
            return true;
        }
    }

}
