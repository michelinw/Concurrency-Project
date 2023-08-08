
package tributary.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import tributary.api.exceptions.ConsumerException;

/**
 *
 * @author Michael Wang
 */
public interface Consumer {
    /**
     * Receive message from the partition
     * @param partitionId
     * @return message
     * @throws ConsumerException
     */
    public Message receive(String partitionId) throws ConsumerException;

    /**
     * Playback messages from the partition
     * @param partitionId
     * @param offset
     * @return
     * @throws ConsumerException
     */
    public ArrayList<Message> playback(String partitionId, int offset) throws ConsumerException;

    /**
     *
     * @return consumer group id
     */
    public String getConsumerGroupId();

    /**
     *
     * @return offset of the partition consumed by the consumer
     */
    public LinkedHashMap<String, Integer> getConsumePartitionsOffset();

    /**
     *
     * @return consumer id
     */
    public String getConsumerId();

}
