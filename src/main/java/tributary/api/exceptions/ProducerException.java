package tributary.api.exceptions;

/**
 *
 * @author Michael Wang
 */
public class ProducerException extends Exception {

    /**
     *
     * @param message
     */
    public ProducerException(String message) {
        super(message);
    }

    /**
     * A class used to represent the exceptions during producer operations
     */
    public static class NotExistTopicException extends ProducerException {

        /**
         *
         * @param message
         */
        public NotExistTopicException(String message) {
            super(message);
        }
    }

    /**
     *
     */
    public static class MandatoryPartitionIdMissingException extends ProducerException {

        /**
         *
         * @param message
         */
        public MandatoryPartitionIdMissingException(String message) {
            super(message);
        }
    }
    /**
     *
     */
    public static class PartitionNotFoundException extends ProducerException {

        /**
         *
         * @param message
         */
        public PartitionNotFoundException(String message) {
            super(message);
        }
    }
    /**
     *
     */
    public static class EventTypeNotMatchToTopicException extends ProducerException {

        /**
         *
         * @param message
         */
        public EventTypeNotMatchToTopicException(String message) {
            super(message);
        }
    }
}
