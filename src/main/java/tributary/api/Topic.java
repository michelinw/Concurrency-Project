
package tributary.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

import tributary.api.exceptions.TopicException;

/**
 *
 * @author Michael Wang
 */
public interface Topic {

    /**
     *
     * @return
     */
    public ConcurrentHashMap<String, ConsumerGroup> getConsumerGroups();

    /**
     *
     * @param partitionId
     * @throws TopicException
     */
    public void createPartition(String partitionId) throws TopicException;

    /**
     * add a partition to the topic
     * @param partition
     */
    public void addPartition(Partition partition) throws TopicException;

    /**
     * return current partitions names
     * @return array list of all partitions names
     */
    public ArrayList<String> getPartitionListing();

    /**
     *
     * @param consumerGroupId
     * @param rebalancePolicy
     */
    public void createConsumerGroup(String consumerGroupId, String rebalancePolicy);

    /**
     *
     * @param
     * @return the topic id
     */

    public String getTopicId();

    /**
     *
     * @param string
     * @returna consumer group by id
     */
    public ConsumerGroup getConsumerGroupId(String string);

    public LinkedHashMap<String, Partition> getPartitions();
}
