package hu.sztaki.ilab.bigdata.common.utils;


import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.carrot2.util.StreamUtils;

/**
 * Class for parsing HTTP responses. Takes care of determining correct encoding
 * and content-type via ContentTypeParser.
 * @author miklos
 */
public class HttpResponseParser {
    private static final Log LOG = LogFactory.getLog(HttpResponseParser.class);

    private static final int BUFFER_SIZE = 8 * 1024;
    private static final String DEFAULT_CHARSET = "ISO-8859-1";

    private TreeMap<String, String> headers = new TreeMap<String, String>();
    private byte[] payload = null;
    private String encoding = null;

    // Excluded header information from metadata field of RawContentRecord.
    private static final String[] excludedHeaderKeys = {
        "set-cookie", "x-powered-by", "server", "connection", "date", "expires"};
    private static final Map<String, Integer> excludeHeaders = new HashMap<String, Integer>();

    public HttpResponseParser() {
        for (int i = 0; i < excludedHeaderKeys.length; i++) {
            excludeHeaders.put(excludedHeaderKeys[i], 1);
        }
    }

    /**
     * Parses input and stores header key/value pairs into variable headers.
     * Body of the response will be put into variable payload.
     * @param uri record's URI
     * @param input InputStream of HTTP response
     * @return true on success, false otherwise
     */
    public boolean process(String uri, InputStream input) {
        try {
            headers.clear();
            PushbackInputStream in = new PushbackInputStream(
                    new BufferedInputStream(input, HttpResponseParser.BUFFER_SIZE),
                    HttpResponseParser.BUFFER_SIZE);
            StringBuffer line = new StringBuffer();
            int responseCode = parseStatusLine(in, line);
            headers.put("http-response", Integer.toString(responseCode));
            // Parse headers.
            parseHeaders(in, line);

            // Set content-type.
            String contentType = (String) headers.get("content-type");
            if (contentType == null) {
                return false;
            }
            ContentTypeParser contentTypeParser = new ContentTypeParser(contentType);
            headers.put("content-type", contentTypeParser.getContentType());
            // Try to find out encoding.
            encoding = (String) headers.get("BUbiNG-guessed-charset".toLowerCase());
            if (encoding == null) {
                encoding = (String) headers.get("ubi-http-equiv-charset");
            }
            // Check if charset guessed from non-content-type headers is
            // correct.
            try {
                if (encoding != null && !Charset.isSupported(encoding)) {
                    encoding = contentTypeParser.getCharset();
                }
            } catch (IllegalCharsetNameException e) {
                encoding = contentTypeParser.getCharset();
            }
            // Still null charset, so set to parsed one from content-type header.
            if (encoding == null) {
                encoding = contentTypeParser.getCharset();
            }
            // Set to the default charset if either encoding is still not available
            // or a charset with an invalid name has been found.
            try {
                if (encoding == null || !Charset.isSupported(encoding)) {
                    encoding = HttpResponseParser.DEFAULT_CHARSET;
                }
            } catch (IllegalCharsetNameException e) {
                LOG.warn("Record for URI " + uri + " has illegal charset name (switching to default): " + encoding + " and " + contentTypeParser.getCharset() + " from header value " + contentType);
                encoding = HttpResponseParser.DEFAULT_CHARSET;
            }

            payload = StreamUtils.readFully(in);
        } catch (IOException ioex) {
            LOG.error("Something went wrong with record for URI " + uri + ":", ioex);
        }

        return true;
    }

    public TreeMap<String, String> getHeaders() {
        return headers;
    }

    public String getEncoding() {
        return encoding;
    }

    public byte[] getPayload() {
        return payload;
    }

    //
    // The below code is taken from Nutch's protocol-http plugin (HttpResponse.java).
    //
    private int parseStatusLine(PushbackInputStream in, StringBuffer line)
            throws IOException {
        readLine(in, line, false);

        int codeStart = line.indexOf(" ");
        int codeEnd = line.indexOf(" ", codeStart + 1);

        // handle lines with no plaintext result code, ie:
        // "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
        if (codeEnd == -1) {
            codeEnd = line.length();
        }

        int code;
        try {
            code = Integer.parseInt(line.substring(codeStart + 1, codeEnd));
        } catch (NumberFormatException e) {
            throw new IOException("bad status line '" + line + "': " + e.getMessage(), e);
        }

        return code;
    }

    private void processHeaderLine(StringBuffer line)
            throws IOException {

        int colonIndex = line.indexOf(":");       // key is up to colon
        if (colonIndex == -1) {
            int i;
            for (i = 0; i < line.length(); i++) {
                if (!Character.isWhitespace(line.charAt(i))) {
                    break;
                }
            }
            if (i == line.length()) {
                return;
            }
            throw new IOException("No colon in header:" + line);
        }
        String key = line.substring(0, colonIndex);

        int valueStart = colonIndex + 1;            // skip whitespace
        while (valueStart < line.length()) {
            int c = line.charAt(valueStart);
            if (c != ' ' && c != '\t') {
                break;
            }
            valueStart++;
        }
        String value = line.substring(valueStart);
        if (!excludeHeaders.containsKey(key.toLowerCase())) {
            headers.put(key.toLowerCase(), value);
        }
    }

    // Adds headers to our headers Metadata
    private void parseHeaders(PushbackInputStream in, StringBuffer line)
            throws IOException {

        while (readLine(in, line, true) != 0) {

            // handle HTTP responses with missing blank line after headers
            int pos;
            if (((pos = line.indexOf("<!DOCTYPE")) != -1) || ((pos = line.indexOf("<HTML")) != -1) || ((pos = line.indexOf("<html")) != -1)) {

                in.unread(line.substring(pos).getBytes("UTF-8"));
                line.setLength(pos);

                try {
                    //TODO: (CM) We don't know the header names here
                    //since we're just handling them generically. It would
                    //be nice to provide some sort of mapping function here
                    //for the returned header names to the standard metadata
                    //names in the ParseData class
                    processHeaderLine(line);
                } catch (Exception e) {
                    // fixme:
                    LOG.error(e);
                }
                return;
            }

            processHeaderLine(line);
        }
    }

    private static int readLine(PushbackInputStream in, StringBuffer line,
            boolean allowContinuedLine)
            throws IOException {
        line.setLength(0);
        for (int c = in.read(); c != -1; c = in.read()) {
            switch (c) {
                case '\r':
                    if (peek(in) == '\n') {
                        in.read();
                    }
                case '\n':
                    if (line.length() > 0) {
                        // at EOL -- check for continued line if the current
                        // (possibly continued) line wasn't blank
                        if (allowContinuedLine) {
                            switch (peek(in)) {
                                case ' ':
                                case '\t':                   // line is continued
                                    in.read();
                                    continue;
                            }
                        }
                    }
                    return line.length();      // else complete
                default:
                    line.append((char) c);
            }
        }
        throw new EOFException();
    }

    private static int peek(PushbackInputStream in) throws IOException {
        int value = in.read();
        in.unread(value);
        return value;
    }
}
