
package tributary.api;

import java.util.LinkedHashMap;
import tributary.api.exceptions.ConsumerException;
import tributary.api.exceptions.ConsumerGroupException;
import tributary.api.exceptions.TopicException;

/**
 *
 * @author Michael Wang
 */
public interface ConsumerGroup {
    /**
     * Set rebalance policy
     * @param rebalancePolicy
     * @throws TopicException
     */
    public void setRebalancePolicy(String rebalancePolicy) throws TopicException;

    /**
     * Add consumer to the consumer group
     * @param consumerId
     * @throws TopicException
     * @throws ConsumerGroupException
     */
    public void addConsumer(String consumerId)
        throws TopicException, ConsumerException, ConsumerGroupException;

    /**
     * Remove consumer from the consumer group
     * @param consumerId
     * @throws TopicException
     * @throws ConsumerGroupException
     */
    public void deleteConsumer(String consumerId)
        throws TopicException, ConsumerGroupException;

    /**
     * Rebalance consumers in the consumer group
     * @throws TopicException
     */
    public void rebalanceConsumers() throws TopicException;

    /**
     *
     * @return consumers in the consumer group
     */
    public LinkedHashMap<String, Consumer> getConsumers();

    /**
     *
     * @return consumer group id
     */
    public String getConsumerGroupId();

    /**
     *
     * @return rebalance policy
     */
    public String getRebalancePolicy();

}
