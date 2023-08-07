
package tributary.cli;

import tributary.api.Topic;
import tributary.api.exceptions.TopicException;
import tributary.api.TributaryFactory;

/**
 * A class used to represent a topic operation.
 */
public class TopicOperation {

    /**
     * Create an instance for a topic.
     *
     * @param topicId The topic id
     * @param payloadType the type of event that go through this topic
     */
    public static final void createTopic(String topicId, String payloadType) {
        TributaryFactory.createTopicInstance(topicId, payloadType);
    }

    /**
     * Get a topic's basic information
     *
     * @param topicId The topic id
     * @return a topic info object defined by topic ID
     */
    public  static Topic getTopicInfo(String topicId) throws TopicException {
        Topic topic = TributaryFactory.getTopicInstance(topicId);
        return topic;
    }
}
