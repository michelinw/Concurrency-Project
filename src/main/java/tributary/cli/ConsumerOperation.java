
package tributary.cli;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import tributary.api.Consumer;
import tributary.api.exceptions.ConsumerGroupException;
import tributary.api.exceptions.TopicException;
import tributary.api.ConsumerGroup;
import tributary.api.Message;
import tributary.api.Topic;
import tributary.api.TributaryFactory;
import tributary.api.exceptions.ConsumerException;

/**
 *
 * A class used to represent a Consumer operation.
 */
public class ConsumerOperation {

    /**
     * Create a consumer in a consumer group.
     *
     * @param consumerGroupId The consumer group id in which we will create consumer
     * @param consumerId The consumer id will be created under the consumer group
     * @throws tributary.api.exceptions.ConsumerGroupException
     * @throws tributary.api.exceptions.TopicException
     */
    public static final void createConsumer(String consumerGroupId, String consumerId)
            throws ConsumerGroupException, TopicException, ConsumerException {

        ConsumerGroup consumerGroup = ConsumerGroupOperation.getConsumerGroup(consumerGroupId);
        if (consumerGroup != null) {
            consumerGroup.addConsumer(consumerId);
        } else {
            throw new ConsumerGroupException.ConsumerGroupNotFoundException("createConsumer: consumer group ["
                + consumerGroupId + "] is not found.");
        }
    }

    /**
     * Delete a consumer in a consumer group..
     *
     * @param consumerGroupId The consumer group id in which we will create consumer
     * @param consumerId The consumer id will be created under the consumer group
     * @throws tributary.api.exceptions.ConsumerGroupException
     * @throws tributary.api.exceptions.TopicException

     */
    public static final void deleteConsumer(String consumerGroupId, String consumerId)
            throws ConsumerGroupException, TopicException {
        ConsumerGroup consumerGroup = ConsumerGroupOperation.getConsumerGroup(consumerGroupId);
        if (consumerGroup != null) {
            consumerGroup.deleteConsumer(consumerId);
        } else {
            throw new ConsumerGroupException.ConsumerGroupNotFoundException("createConsumer: consumer group ["
                + consumerGroupId + "] is not found.");
        }

    }

    public static final void deleteConsumer(String consumerId)
            throws ConsumerGroupException, TopicException, ConsumerException {

        ConcurrentHashMap<String, Topic> topics = TributaryFactory.getTopics();
        for (Topic topic : topics.values()) {
            ConcurrentHashMap<String, ConsumerGroup> consumerGroups = topic.getConsumerGroups();
            for (ConsumerGroup consumerGroup : consumerGroups.values()) {
                LinkedHashMap<String, Consumer> consumers = consumerGroup.getConsumers();
                if (consumers.containsKey(consumerId)) {
                    consumerGroup.deleteConsumer(consumerId);
                    return;
                }
            }
        }
        throw new ConsumerException.ConsumerNotFoundException("deleteConsumer: consumer  ["
            + consumerId + "] is not found.");
    }

    public static final Consumer getConsumer(String consumerId) {
        ConcurrentHashMap<String, Topic> topics = TributaryFactory.getTopics();
        for (Topic topic : topics.values()) {
            ConcurrentHashMap<String, ConsumerGroup> consumerGroups = topic.getConsumerGroups();
            for (ConsumerGroup consumerGroup : consumerGroups.values()) {
                LinkedHashMap<String, Consumer> consumers = consumerGroup.getConsumers();
                if (consumers.containsKey(consumerId)) {
                    Consumer consumer = consumers.get(consumerId);
                    return consumer;
                }
            }
        }
        return null;
    }

    public static final Message receiveEvent(String consumerId, String partitionId) throws ConsumerException {
        Consumer consumer = getConsumer(consumerId);
        if (consumer != null) {
            return consumer.receive(partitionId);
        }
        throw new ConsumerException.ConsumerNotFoundException(
                "receiveEvent: consumer  [" + consumerId + "] is not found.");
    }

    public static final ArrayList<Message> playbackEvent(String consumerId, String partitionId, int offset)
            throws ConsumerException {
        Consumer consumer = getConsumer(consumerId);
        if (consumer != null) {
            return consumer.playback(partitionId, offset);
        }
        throw new ConsumerException.ConsumerNotFoundException("playbackEvent: consumer  ["
            + consumerId + "] is not found.");
    }
}
