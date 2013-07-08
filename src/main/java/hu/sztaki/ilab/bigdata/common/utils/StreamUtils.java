package hu.sztaki.ilab.bigdata.common.utils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

public class StreamUtils {

    
    private static byte MASK_THREE_BYTE_CHAR = (byte) (0xE0);
    private static byte MASK_TWO_BYTE_CHAR = (byte) (0xC0);
    private static byte MASK_TOPMOST_BIT = (byte) (0x80);
    private static byte MASK_BOTTOM_SIX_BITS = (byte) (0x1F);
    private static byte MASK_BOTTOM_FIVE_BITS = (byte) (0x3F);
    private static byte MASK_BOTTOM_FOUR_BITS = (byte) (0x0F);
    
    public static String nonBufferedReadLine(DataInputStream in) throws IOException {
            StringBuilder retString = new StringBuilder();

            boolean keepReading = true;
            try {
                    do {
                            char thisChar = 0;
                            byte readByte = in.readByte();                                

                            // check to see if it's a multibyte character
                            if ((readByte & MASK_THREE_BYTE_CHAR) == MASK_THREE_BYTE_CHAR) {
                                    // need to read the next 2 bytes
                                    if (in.available() < 2) {
                                            // treat these all as individual characters
                                            retString.append((char) readByte);
                                            int numAvailable = in.available();
                                            for (int i = 0; i < numAvailable; i++) {
                                                    retString.append((char) (in.readByte()));
                                            }
                                            continue;
                                    }
                                    byte secondByte = in.readByte();
                                    byte thirdByte = in.readByte();
                                    // ensure the topmost bit is set
                                    if (((secondByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT)
                                                    || ((thirdByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT)) {
                                            // treat these as individual characters
                                            retString.append((char) readByte);
                                            retString.append((char) secondByte);
                                            retString.append((char) thirdByte);
                                            continue;
                                    }
                                    int finalVal = (thirdByte & MASK_BOTTOM_FIVE_BITS) + 64
                                                    * (secondByte & MASK_BOTTOM_FIVE_BITS) + 4096
                                                    * (readByte & MASK_BOTTOM_FOUR_BITS);
                                    thisChar = (char) finalVal;
                            } else if ((readByte & MASK_TWO_BYTE_CHAR) == MASK_TWO_BYTE_CHAR) {
                                    // need to read next byte
                                    if (in.available() < 1) {
                                            // treat this as individual characters
                                            retString.append((char) readByte);
                                            continue;
                                    }
                                    byte secondByte = in.readByte();
                                    if ((secondByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT) {
                                            retString.append((char) readByte);
                                            retString.append((char) secondByte);
                                            continue;
                                    }
                                    int finalVal = (secondByte & MASK_BOTTOM_FIVE_BITS) + 64
                                                    * (readByte & MASK_BOTTOM_SIX_BITS);
                                    thisChar = (char) finalVal;
                            } else {
                                    // interpret it as a single byte
                                    thisChar = (char) readByte;
                            }

                            if (thisChar == '\n') {
                                    keepReading = false;
                            } else {
                                    retString.append(thisChar);
                            }
                    } while (keepReading);
            } catch (EOFException eofEx) {
                    return null;
            }

            if (retString.length() == 0) {
                    return "";
            }

            return retString.toString();
    }
        
}
