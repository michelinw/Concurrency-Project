
package tributary.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import tributary.api.ConsumerGroup;
import tributary.api.Partition;
import tributary.api.Topic;
import tributary.api.exceptions.TopicException;


public class TopicImpl implements Topic  {

    /**
     * topic id
     */
    private String topicId;

    /**
     * topic event type
     */
    private String eventPayloadType;

    /**
     * partitions in the topic
     */
    private LinkedHashMap<String, Partition> partitions;

    /**
     * consumerGroups in the topic
     */
    private ConcurrentHashMap<String, ConsumerGroup> consumerGroups;
    /**
     *
     * @param topicId
     * @param eventPayloadType
     */
    public TopicImpl(String topicId, String eventPayloadType) {
        this.topicId = topicId;
        this.eventPayloadType = eventPayloadType;
        this.partitions = new LinkedHashMap<>();
        this.consumerGroups = new ConcurrentHashMap<>();
    }

    /**
     *
     * @return
     */
    public String getTopicId() {
        return topicId;
    }

    /**
     *
     * @return
     */
    public String getEventPayloadType() {
        return eventPayloadType;
    }

    /**
     *
     * @return
     */
    public LinkedHashMap<String, Partition> getPartitions() {
        return partitions;
    }

    /**
     *
     * @return
     */
    public ConcurrentHashMap<String, ConsumerGroup> getConsumerGroups() {
        return consumerGroups;
    }

    @Override
    public void createPartition(String partitionId) throws TopicException {
        PartitionImpl partition = new PartitionImpl(partitionId);
        this.addPartition(partition);
    }

    @Override
    public void addPartition(Partition partition) throws TopicException {
        partitions.put(((PartitionImpl) partition).getPartitionId(), partition);
        for (String key : consumerGroups.keySet()) {
            consumerGroups.get(key).rebalanceConsumers();
        }
    }

    @Override
    public void createConsumerGroup(String consumerGroupId, String rebalancePolicy) {
        ConsumerGroup consumerGroup = new ConsumerGroupImpl(topicId, consumerGroupId, rebalancePolicy);
        consumerGroups.put(consumerGroupId, consumerGroup);
    }

    @Override
    public ArrayList<String> getPartitionListing() {
        ArrayList<String> partitionList = new ArrayList<>();
        for (String partitionKey : partitions.keySet()) {
            partitionList.add(partitionKey);
        }
        return partitionList;
    }

    @Override
    public final String toString() {
        String str = "Topic : " + topicId + ", event type:" + eventPayloadType + "\n";
        for (String partitionKey : partitions.keySet()) {
            Partition partition = partitions.get(partitionKey);
            str = str + partition.toString();
        }
        return str;
    }

    @Override
    public ConsumerGroup getConsumerGroupId(String string) {
        return consumerGroups.get(string);
    }

}

