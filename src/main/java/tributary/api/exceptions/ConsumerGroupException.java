
package tributary.api.exceptions;

/**
 *
 * A class used to represent the exceptions during consumer group operation
 */
public class ConsumerGroupException extends Exception {

    /**
     *
     * @param message
     */
    public ConsumerGroupException(String message) {
        super(message);
    }

    /**
     * A class used to represent the exceptions during consumer group operations
     */
    public static class ConsumerGroupNotFoundException extends ConsumerGroupException {

        /**
         *
         * @param message
         */
        public ConsumerGroupNotFoundException(String message) {
            super(message);
        }
    }

    /**
     *
     */
    public static class TooManyConsumersInConsumerGroupException extends ConsumerGroupException {

        /**
         *
         * @param message
         */
        public TooManyConsumersInConsumerGroupException(String message) {
            super(message);
        }
    }

    /**
     *
     */
    public static class ConsumerNotExistsInConsumerGroupException extends ConsumerGroupException {

        /**
         *
         * @param message
         */
        public ConsumerNotExistsInConsumerGroupException(String message) {
            super(message);
        }
    }

}
