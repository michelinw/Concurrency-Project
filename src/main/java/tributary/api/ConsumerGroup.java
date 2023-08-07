
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
     *
     * @param rebalancePolicy
     * @throws TopicException
     */
    public void setRebalancePolicy(String rebalancePolicy) throws TopicException;

    /**
     *
     * @param consumerId
     * @throws TopicException
     * @throws ConsumerGroupException
     */
    public void addConsumer(String consumerId)
        throws TopicException, ConsumerException, ConsumerGroupException;

    /**
     *
     * @param consumerId
     * @throws TopicException
     * @throws ConsumerGroupException
     */
    public void deleteConsumer(String consumerId)
        throws TopicException, ConsumerGroupException;

    /**
     *
     * @throws TopicException
     */
    public void rebalanceConsumers() throws TopicException;

    /**
     *
     * @return
     */
    public LinkedHashMap<String, Consumer> getConsumers();

}
