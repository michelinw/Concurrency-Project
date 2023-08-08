
package tributary.api.exceptions;

/**
 *
 * @author Michael Wang
 */
public class ConsumerException extends Exception {

    /**
     *
     * @param message
     */
    public ConsumerException(String message) {
        super(message);
    }

    /**
     * A class used to represent the exceptions during consumer operations
     */
    public static class NoUnreadMessageException extends ConsumerException {

        /**
         *
         * @param message
         */
        public NoUnreadMessageException(String message) {
            super(message);
        }
    }

    /**
     *
     */
    public static class ConsumerNotFoundException extends ConsumerException {

        /**
         *
         * @param message
         */
        public ConsumerNotFoundException(String message) {
            super(message);
        }
    }

    /**
     *
     */
    public static class NotAllocatdPartitionException extends ConsumerException {

        /**
         *
         * @param message
         */
        public NotAllocatdPartitionException(String message) {
            super(message);
        }
    }

    /**
     *
     */
    public static class PlaybackOffsetIncorrectException extends ConsumerException {

        /**
         *
         * @param message
         */
        public PlaybackOffsetIncorrectException(String message) {
            super(message);
        }
    }
}
