package tributary.api.exceptions;

/**
 *
 * @author Michael Wang
 */
public class MessageException extends Exception {

    /**
     *
     * @param message
     */
    public MessageException(String message) {
        super(message);
    }

    /**
     * Unsupported serialization exception
     */
    public static class SerializationException extends ConsumerException {

        /**
         *
         * @param message error message
         */
        public SerializationException(String message) {
            super(message);
        }
    }

}
