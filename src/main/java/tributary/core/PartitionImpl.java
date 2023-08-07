package tributary.core;

import java.util.concurrent.CopyOnWriteArrayList;
import tributary.api.Message;
import tributary.api.Partition;


public class PartitionImpl implements Partition {

    /**
     *
     */
    private String partitionId;

    /**
     *
     */
    private CopyOnWriteArrayList<Message> messages;

    /**
     *
     * @return
     */
    public String getPartitionId() {
        return partitionId;
    }

    /**
     *
     * @return
     */
    public CopyOnWriteArrayList<Message> getMessages() {
        return messages;
    }

    public PartitionImpl(String patritionId) {
        this.partitionId = patritionId;
        this.messages = new CopyOnWriteArrayList<>();
    }


    @Override
    public void addMessage(Message msg) {
        messages.add(msg);
    }

    @Override
    public Message peekMessage(int messageOffset) {
        int totalMsg = messages.size();
        Message msg = null;
        if (messageOffset < totalMsg) {
            msg = messages.get(messageOffset);
        }
        return msg;
    }

    @Override
    public final String toString() {
        String str = "Partition : " + this.getPartitionId() + ", Total events:" + messages.size() + "\n";
        for (int i = 0; i < messages.size(); i++) {
            str = str + String.valueOf(i) + "->" + messages.get(i).toString() + "\n";
        }
        return str;
    }

}
