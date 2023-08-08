
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
     *
     * @param partitionId
     * @return message
     * @throws ConsumerException
     */
    public Message receive(String partitionId) throws ConsumerException;

    /**
     *
     * @param partitionId
     * @param offset
     * @return
     * @throws ConsumerException
     */
    public ArrayList<Message> playback(String partitionId, int offset) throws ConsumerException;

    /**
     *
     * @return String
     */
    public String getConsumerGroupId();

    /**
     *
     * @return String
     */
    public LinkedHashMap<String, Integer> getConsumePartitionsOffset();

        /**
     * get consumer id
     * @return consumer id
     */
    public String getConsumerId();

}
