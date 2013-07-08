package hu.sztaki.ilab.bigdata.common.tokenize;


/**
 * Class representing a single token extracted from raw text.
 * @author miklos
 */
public class Token {

    private String value;
    private TokenType type;

    public Token() {
    }

    public Token(String value) {
        this.value = value;
    }
    
    public Token(String value, TokenType type) {
        this.value = value;
        this.type = type;
    }

    public TokenType getType() {
        return type;
    }

    public void setType(TokenType type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value + " (" + type + ")";
    }
}
