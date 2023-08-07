package tributary.api.serialization;


/**
 *  String encoding  to UTF8 bytes[].
 */
public class IntegerSerializer implements Serializer<Integer> {

    @Override
    public byte[] serialize(Integer data) {
        if (data == null)
            return null;

        return new byte[] {
            (byte) (data >>> 24),
            (byte) (data >>> 16),
            (byte) (data >>> 8),
            data.byteValue()
        };
    }

}
