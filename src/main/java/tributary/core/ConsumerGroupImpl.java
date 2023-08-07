package tributary.core;

import tributary.api.TributaryFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import tributary.api.Consumer;
import tributary.api.ConsumerGroup;
import tributary.api.constants.ConsumerGroupRebalancePolicy;
import tributary.api.Partition;
import tributary.api.exceptions.ConsumerException;
import tributary.api.exceptions.ConsumerGroupException;
import tributary.api.exceptions.TopicException;


public class ConsumerGroupImpl implements ConsumerGroup {

    /**
     *
     */
    private String topicId;

    /**
     *
     */
    private String consumerGroupId;

    /**
     *
     */
    private String rebalancePolicy;


    private LinkedHashMap<String, Consumer> consumers;

    private ConcurrentHashMap<String, Integer> allPartitionsOffset;

    /**
     *
     * @param topicId
     * @param consumerGroupId
     * @param policy
     */
    public ConsumerGroupImpl(String topicId, String consumerGroupId, String policy) {
        this.topicId = topicId;
        this.consumerGroupId = consumerGroupId;
        this.rebalancePolicy = policy;
        this.consumers = new LinkedHashMap<>();
        this.allPartitionsOffset = new ConcurrentHashMap<>();
    }

    /**
     *
     * @return
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     *
     * @return
     */
    public String getRebalancePolicy() {
        return rebalancePolicy;
    }

    private int getPartionsSize() throws TopicException {
        TopicImpl topic = (TopicImpl) TributaryFactory.getTopicInstance(topicId);
        LinkedHashMap<String, Partition> parts = topic.getPartitions();
        return parts.size();
    }


    // upon rebalancing, the consumer group will need to know the current offset of all partitions
    // this method will refresh the allPartitionsOffset map
    // this method is called by rebalanceConsumers()
    // this method is called by addConsumer()
    // this method is called by deleteConsumer()
    // this method is called by setRebalancePolicy()
    // this method is called by getConsumers()
    // store temp offset in allPartitionsOffset
    private void refreshAllPartitionOffset() {
        allPartitionsOffset.clear();
        for (String consumerKey : consumers.keySet()) {
            ConsumerImpl consumer = (ConsumerImpl) consumers.get(consumerKey);
            LinkedHashMap<String, Integer> consumePartitionsOffset = consumer.getConsumePartitionsOffset();
            for (String partitionKey : consumePartitionsOffset.keySet()) {
                Integer offset = consumePartitionsOffset.get(partitionKey);
                allPartitionsOffset.put(partitionKey, (offset == null) ? Integer.valueOf(0) : offset);
            }
        }
    }

    @Override
    public void rebalanceConsumers() throws TopicException {
        int consumerSize = consumers.size();
        if (consumerSize == 0) return;
        TopicImpl topic = (TopicImpl) TributaryFactory.getTopicInstance(topicId);
        LinkedHashMap<String, Partition> partitions = topic.getPartitions();
        int partSize = partitions.size();
        int baseSize = partSize / consumerSize;
        int remainder = partSize % consumerSize;
        int consumerIndex = 0;
        int startPartIndex = 0;
        int endPartIndex = 0;

        ArrayList<String> partitionKeys = new ArrayList<>(partitions.keySet());

        for (String consumerKey : consumers.keySet()) {
            ConsumerImpl consumer = (ConsumerImpl) consumers.get(consumerKey);
            LinkedHashMap<String, Partition> consumerPartitions = consumer.getConsumePartitions();
            LinkedHashMap<String, Integer> consumePartitionsOffset = consumer.getConsumePartitionsOffset();

            //Step 1: clear current consumer -> partition relationship
            consumerPartitions.clear();
            consumePartitionsOffset.clear();

            if (rebalancePolicy.equals(ConsumerGroupRebalancePolicy.POLICY_RANGE)) {
                //Step 2: add the parttion within the range to cusumer's relation
                startPartIndex = endPartIndex;
                endPartIndex = startPartIndex + baseSize;
                if (consumerIndex < remainder) {
                    endPartIndex++;
                }
                for (int i = startPartIndex; i < endPartIndex; i++) {
                    String partitionKey = partitionKeys.get(i);
                    Integer offset = allPartitionsOffset.get(partitionKey);
                    consumerPartitions.put(partitionKey, partitions.get(partitionKey));
                    consumePartitionsOffset.put(partitionKey, (offset == null) ? Integer.valueOf(0) : offset);
                }
            } else {
                //Step 2: add the parttion in round robin range to cusumer's relation
                for (int i = 0; i < partSize; i++) {
                    if (consumerIndex == i % consumerSize) {
                        String partitionKey = partitionKeys.get(i);
                        Integer offset = allPartitionsOffset.get(partitionKey);
                        consumerPartitions.put(partitionKey, partitions.get(partitionKey));
                        consumePartitionsOffset.put(partitionKey, (offset == null) ? Integer.valueOf(0) : offset);
                    }
                }
            }
            consumerIndex++;
        }
    }

    // create consumer, refresh offset, add into consumers, rebalance
    @Override
    public void addConsumer(String consumerId) throws TopicException, ConsumerGroupException, ConsumerException {
        if (consumers.size() < getPartionsSize()) {
            if (!consumers.containsKey(consumerId)) {
                Consumer consumer = new ConsumerImpl(consumerId, consumerGroupId);
                refreshAllPartitionOffset();
                consumers.put(consumerId, consumer);
                rebalanceConsumers();
            }
        } else {
            throw new ConsumerGroupException.TooManyConsumersInConsumerGroupException(
                "createConsumer: Too many consumers for cusomer group [" + this.consumerGroupId
                    + "] when creating consumer [" + consumerId + "]");
        }
    }

    @Override
    public void deleteConsumer(String consumerId) throws TopicException, ConsumerGroupException {
        if (consumers.containsKey(consumerId)) {
            refreshAllPartitionOffset();
            consumers.remove(consumerId);
            rebalanceConsumers();
        } else {
            throw new ConsumerGroupException.ConsumerNotExistsInConsumerGroupException(
                "deleteConsumer: Consumer [" + consumerId + "] dose not exist in cusomer group ["
                + this.consumerGroupId + "]");
        }
    }

    @Override
    public void setRebalancePolicy(String rebalancePolicy) throws TopicException {
        if (rebalancePolicy != this.rebalancePolicy) {
            this.rebalancePolicy = rebalancePolicy;
            refreshAllPartitionOffset();
            rebalanceConsumers();
        }
    }

    @Override
    public LinkedHashMap<String, Consumer> getConsumers() {
        return consumers;
    }

     @Override
    public final String toString() {
        String str = "Consumer Group: " + consumerGroupId + ", Topic Id: "
            + topicId + " rebalance policy: "
            + rebalancePolicy + "\n";
        for (String consumerKey : consumers.keySet()) {
            Consumer consumer = consumers.get(consumerKey);
            str = str + consumer.toString();
        }
        return str;
    }
}
