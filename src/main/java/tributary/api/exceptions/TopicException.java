package tributary.api.exceptions;

/**
 *
 * A class used to represent the exceptions during topic operation
 */
public class TopicException extends Exception {

    /**
     *
     * @param message
     */
    public TopicException(String message) {
        super(message);
    }

    /**
     * A class used to represent the exceptions during topic operations
     */
    public static class TopicNotFoundException extends TopicException {

        /**
         *
         * @param message
         */
        public TopicNotFoundException(String message) {
            super(message);
        }
    }

}
