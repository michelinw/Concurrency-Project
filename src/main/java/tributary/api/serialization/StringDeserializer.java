package tributary.api.serialization;

import java.io.UnsupportedEncodingException;

/**
 *  String deserializer class encoding defaults to UTF8
 */
public class StringDeserializer implements Deserializer<String> {
    private String encoding = "UTF8";

    /**
     * String deserializer class encoding defaults to UTF8
     * @param encoding
     * @return String deserialized from byte array
     */
    @Override
    public String deserialize(byte[] data) {
        try {
            if (data == null)
                return null;
            else
                return new String(data, encoding);
        } catch (UnsupportedEncodingException e) {

        }
        return null;
    }

}
