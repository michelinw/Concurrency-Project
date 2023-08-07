package tributary.api;

import tributary.api.exceptions.ProducerException;
import tributary.api.exceptions.TopicException;

/**
 *
 * @author Michael Wang
 */
public interface Producer {
    /**
     *
     * @param topicId
     * @param eventContent
     * @param partitionId
     * @throws ProducerException
     * @throws TopicException
     */
    public void send(String topicId, Object eventContent, String partitionId)
        throws ProducerException, TopicException;

    /**
     *
     * @return
     */
    public String getProducerId();

    /**
     *
     * @return
     */
    public String getAllocationType();
}
