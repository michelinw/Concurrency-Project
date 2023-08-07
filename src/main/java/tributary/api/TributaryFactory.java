
package tributary.api;

import java.util.concurrent.ConcurrentHashMap;
import tributary.api.exceptions.TopicException;
import tributary.core.ProducerImpl;
import tributary.core.TopicImpl;

/**
 *
 * @author Michael Wang
 *
 */
public class TributaryFactory {

    private static final ConcurrentHashMap<String, Topic> TOPICS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Producer> PRODUCERS = new ConcurrentHashMap<>();
    // we could use this factory to create everything, but then code would be repeated. If we wish to extend this api
    //  we can implement, e.g., a consumer group hashmap here, create some more vairables to check for types,
    // can use obj -
    /**
     *
     * @param topicId
     * @param eventPayloadType
     */
    public static final void createTopicInstance(String topicId, String eventPayloadType) {
        TopicImpl topic = new TopicImpl(topicId, eventPayloadType);
        TOPICS.put(topicId, topic);
    }

    /**
     *
     * @param topicId
     * @return
     * @throws TopicException.TopicNotFoundException
     */
    public static final Topic getTopicInstance(String topicId) throws TopicException.TopicNotFoundException {
        Topic topic = TOPICS.get(topicId);
        if (topic == null) {
            throw new TopicException.TopicNotFoundException("getTopicInstance: Topic [" + topicId + "] is not found");
        }
        return topic;
    }

    /**
     * delete topic instance
     *
     * @param topicId topic id
     */
    public static final void deleteTopicInstance(String topicId) {
        TOPICS.remove(topicId);
    }

    /**
     * Return the current topics
     *
     * @return all topics in hash map
     */
    public static final ConcurrentHashMap<String, Topic> getTopics() {
        return TOPICS;
    }

    /**
     *
     * @param producerId
     * @param payloadType
     * @param allocationType
     */
    public static final void createProducerInstance(String producerId, String payloadType, String allocationType) {
        Producer producer = new ProducerImpl(producerId, payloadType, allocationType);
        PRODUCERS.put(producerId, producer);
    }

    /**
     *
     * @param producerId
     * @return
     */
    public static final Producer getProducerInstance(String producerId) {
        return PRODUCERS.get(producerId);
    }

    /**
     *
     * @param producerId
     * @return
     */
    public static final Producer deleteProducerInstance(String producerId) {
        return PRODUCERS.remove(producerId);
    }

    /**
     *
     * @return all producers in hash map
     */
    public static final ConcurrentHashMap<String, Producer> getProducers() {
        return PRODUCERS;
    }
}
