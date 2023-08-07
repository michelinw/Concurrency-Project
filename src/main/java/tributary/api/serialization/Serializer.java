package tributary.api.serialization;

/**
 * An interface for converting objects to bytes.
 *
 * @param <T> Type to be serialized from.
 */
public interface Serializer<T> {


    /**
     * Convert {@code data} into a byte array.
     *
     * @param data typed data
     * @return serialized bytes
     */
    byte[] serialize(T data);

}
