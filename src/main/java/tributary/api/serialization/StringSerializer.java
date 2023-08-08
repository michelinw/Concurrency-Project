package tributary.api.serialization;

import java.io.UnsupportedEncodingException;


/**
 *  String encoding  to UTF8 bytes[].
 */
public class StringSerializer implements Serializer<String> {
    private final String encoding = "UTF8";

    /**
     * Serialize String to byte array
     * @param data
     * @return byte[] data in UTF8 encoding
     */
    @Override
    public byte[] serialize(String data) {

        try {
            if (data == null)
                return null;
            else
                return data.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {

        }
        return null;

    }

}
