package hu.sztaki.ilab.bigdata.common.utils;

// Class for parsing content-type and character set specification from a
// Content-Type header.
public class ContentTypeParser {

    private String headerValue;
    private String contentType;
    private String charset;

    public ContentTypeParser() {
    }

    public ContentTypeParser(String value) {
        setHeaderValue(value);
        parseContentType();
    }

    public String getHeaderValue() {
        return headerValue;
    }

    public void setHeaderValue(String value) {
        headerValue = value;
    }

    public String getCharset() {
        return charset;
    }

    public String getContentType() {
        return contentType;
    }

    public void parseContentType(String value) {
        setHeaderValue(value);
        parseContentType();
    }

    public void parseContentType() {
        String[] parts = headerValue.split(";");
        boolean charsetFound = false;
        for (String part : parts) {
            String trimmedPart = part.trim();
            if (trimmedPart.startsWith("charset")) {
                String tmpCharset = trimmedPart.substring(7).trim();
                // A charset name must begin with either a letter or a digit.
                while (tmpCharset.length() > 0) {
                    char firstChar = tmpCharset.charAt(0);
                    if (!Character.isDigit(firstChar) &&
                            !Character.isLetter(firstChar)) {
                        tmpCharset = tmpCharset.substring(1);
                    } else {
                        break;
                    }
                }
                if (tmpCharset.length() == 0) {
                    break;
                }
                // Strip anything from the string which is not allowed in character
                // set names.
                StringBuffer buffer = new StringBuffer();
                for (int i = 0; i < tmpCharset.length(); ++i) {
                    char c = tmpCharset.charAt(i);
                    if (Character.isDigit(c) || Character.isLetter(c) ||
                            c == '-' || c == '.' || c == ':' || c == '_') {
                        buffer.append(c);
                    }
                }
                charset = buffer.toString();
                charsetFound = true;
                break;
            }
        }
        if (!charsetFound) {
            charset = "ISO-8859-1";
        }
        charset.toUpperCase();
        // Correct known wrong charset names.
        charset = charset.replaceFirst("ISO8859", "ISO-8859");
        // Set content-type.
        contentType = parts[0].toLowerCase();
        // Replace wrong HTML mime-type.
        if ("text/htm".equals(contentType)) {
            contentType = "text/html";
        }
    }
}
