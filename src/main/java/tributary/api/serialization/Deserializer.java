package tributary.api.serialization;

/**
 * An interface for converting bytes to objects.
 *
 * @param <T> Type to be deserialized into.
 */
public interface Deserializer<T> {


    /**
     * Deserialize a record value from a byte array into a value or object.
     * @param data serialized bytes; may be null; implementations are
     * recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    T deserialize(byte[] data);

}
