
package tributary.api;

import java.util.concurrent.CopyOnWriteArrayList;

import tributary.api.exceptions.TopicException;

/**
 *
 * @author Michael Wang
 */
public interface Partition {
    /**
     *
     * @param msg
     */
    public void addMessage(Message msg);

    /**
     *
     * @param messageOffset
     * @return
     */
    public Message peekMessage(int messageOffset);

    public CopyOnWriteArrayList<Message> getMessages() throws TopicException;
}
