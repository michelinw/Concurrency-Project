package tributary.cli;

import java.util.ArrayList;
import tributary.api.Topic;
import tributary.api.TributaryFactory;
import tributary.api.exceptions.TopicException;

/**
 * A class used to represent a partition operation.
 */
public class PartitionOperation {

    /**
     * Create a partition in a topic.
     *
     * @param topicId The topic id in which we will create partition
     * @param partitionId The partition id will be created under the topic

     */
    public static final void createPartition(String topicId, String partitionId) throws TopicException {
        Topic topic = TributaryFactory.getTopicInstance(topicId);
        topic.createPartition(partitionId);
    }

    /**
     * retrieve the partition's information under a topic
     *
     * @param topicId The topic id
     * @return a string array contains ids of all partitions
     */
    public static final ArrayList<String> getPartitionInfo(String topicId) throws TopicException {
        Topic topic = TributaryFactory.getTopicInstance(topicId);
        return topic.getPartitionListing();
    }
}
