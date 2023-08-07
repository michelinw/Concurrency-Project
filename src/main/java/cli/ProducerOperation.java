
package cli;


import tributary.api.Producer;
import tributary.api.TributaryFactory;
import tributary.api.exceptions.ProducerException;
import tributary.api.exceptions.TopicException;


/**
 *
 * A class used to represent a producer operation.
 */
public class ProducerOperation {

    /**
     * Create a producer
     *
     * @param producerId The producer id
     * @param payloadType The payload event type for the producer
     * @param allocationType The producer allocation type
     */
    public static final void createProducer(String producerId,
            String payloadType, String allocationType) {
        TributaryFactory.createProducerInstance(producerId, payloadType, allocationType);
    }

    /**
     * retrieve a consumer group .
     *
     * @param consumerGroupId The consumer group id will be created under the topic
     * @return an instance of consumer for the consumer group id, null
     */
    public static final void sendEvent(String producerId, String topicId, String eventContent, String partitionId)
            throws ProducerException, TopicException {
        Producer producer = TributaryFactory.getProducerInstance(producerId);
        boolean isInteger = false;
        try {
            Integer tmp = Integer.valueOf(eventContent);
            isInteger = true;
        } catch (Exception ex) {

        }

        if (isInteger) {
            Integer ieventContent = Integer.valueOf(eventContent);
            producer.send(topicId, ieventContent, partitionId);
        } else {
            producer.send(topicId, eventContent, partitionId);
        }
    }

    public  static Producer getProducer(String producerId) {
        Producer producer = TributaryFactory.getProducerInstance(producerId);
        return producer;
    }
}
