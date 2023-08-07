package tributary.api.serialization;

public class IntegerDeserializer implements Deserializer<Integer> {


    public Integer deserialize(byte[] data) {
        if (data == null)
            return null;

        int value = 0;
        for (byte b : data) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

}
