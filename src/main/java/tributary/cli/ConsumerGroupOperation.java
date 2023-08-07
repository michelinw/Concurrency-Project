
package tributary.cli;

import java.util.concurrent.ConcurrentHashMap;
import tributary.api.ConsumerGroup;
import tributary.api.Topic;
import tributary.api.TributaryFactory;
import tributary.api.exceptions.TopicException;

/**
 * A class used to represent a Consumer Group operation.
 */
public class ConsumerGroupOperation {

    /**
     * Create a consumer group in a topic.
     *
     * @param topicId The topic id in which we will create consumer group
     * @param consumerGroupId The consumer group id will be created under the topic
     * @param rebalancePolicy The consumer group's re-balance policy defined in class ConsumerGroupRebalancePolicy.
     * @throws tributary.api.exceptions.TopicException
     */
    public static final void createConsumerGroup(String topicId, String consumerGroupId, String rebalancePolicy)
            throws TopicException {
        Topic topic = TributaryFactory.getTopicInstance(topicId);
        topic.createConsumerGroup(consumerGroupId, rebalancePolicy);
    }

    /**
     * retrieve a consumer group .
     *
     * @param consumerGroupId The consumer group id will be created under the topic
     * @return an instance of consumer for the consumer group id, null
     */
    public static final ConsumerGroup getConsumerGroup(String consumerGroupId) {

        ConcurrentHashMap<String, Topic> topics = TributaryFactory.getTopics();
        for (Topic topic : topics.values()) {
            ConcurrentHashMap<String, ConsumerGroup> consumerGroups = topic.getConsumerGroups();
            if (consumerGroups.containsKey(consumerGroupId)) {
                ConsumerGroup consumerGroup = consumerGroups.get(consumerGroupId);
                return consumerGroup;
            }
        }
        return null;
    }

    public static final void setConsumerGroupRebalancing(String consumerGroupId, String rebalancingPolicy)
            throws TopicException {
        ConsumerGroup consumerGroup = getConsumerGroup(consumerGroupId);
        consumerGroup.setRebalancePolicy(rebalancingPolicy);
    }
}



