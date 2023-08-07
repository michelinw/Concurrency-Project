package tributary.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import tributary.api.Consumer;
import tributary.api.Message;
import tributary.api.Partition;
import tributary.api.exceptions.ConsumerException;


public class ConsumerImpl implements Consumer {
    /**
     * Store consumer's id
     */
    private String consumerId;

    /**
     * Store consumer's group id
     */
    private String consumerGroupId;

    /**
     * current consuming partitions for the consumer
     */
    private LinkedHashMap<String, Partition> consumePartitions;

    /**
     * current consuming partitions offset for the consumer
     */
    private LinkedHashMap<String, Integer> consumePartitionsOffset;

    /**
     * Constructor
     * @param consumerId  consumer id
     * @param consumerGroupId  consumer belong to consumer group
     */
    public ConsumerImpl(String consumerId, String consumerGroupId) {
        this.consumerId = consumerId;
        this.consumerGroupId = consumerGroupId;
        this.consumePartitions = new LinkedHashMap<>();
        this.consumePartitionsOffset = new LinkedHashMap<>();
    }

    /**
     * get consumer id
     * @return consumer id
     */
    public String getConsumerId() {
        return consumerId;
    }
    /**
     *get consumer related consumer group id
     * @return consumer group id
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     * get consuming partitions
     * @return  partiiton's hash map
     */
    public LinkedHashMap<String, Partition> getConsumePartitions() {
        return consumePartitions;
    }

    /**
     * get consuming partitions offset
     * @return consuming partitions offset hash map
     */
    public LinkedHashMap<String, Integer> getConsumePartitionsOffset() {
        return consumePartitionsOffset;
    }

    @Override
    public Message receive(String partitionId) throws ConsumerException {
        Message msg = null;
        if (consumePartitions.containsKey(partitionId)) {
            PartitionImpl partition = (PartitionImpl) consumePartitions.get(partitionId);
            Integer offset = consumePartitionsOffset.get(partitionId);
            if (offset == null) offset = 0;
            int msgsSize = partition.getMessages().size();
            if (msgsSize > 0 && offset >= 0 && offset < msgsSize) {
                msg = partition.peekMessage(offset);
                consumePartitionsOffset.put(partitionId, 1 + offset);
            } else {
                if (offset == msgsSize) {
                    throw new ConsumerException.NoUnreadMessageException(
                        "Receive: There is no unread message in the given partition [" + partitionId + "]");
                } else {
                    throw new ConsumerException.PlaybackOffsetIncorrectException(
                        "Receive: The offset [" + offset + "] exceed the range of the given partition ["
                        + partitionId + "]");
                }
            }
        } else {
            throw new ConsumerException.NotAllocatdPartitionException("Receive: The consumer ["
                + consumerId + "] has not allocated to the given partition [" + partitionId + "]");
        }
        return msg;
    }

    @Override
    public ArrayList<Message> playback(String partitionId, int offset) throws ConsumerException {
        if (consumePartitions.containsKey(partitionId)) {
            PartitionImpl partition = (PartitionImpl) consumePartitions.get(partitionId);
            int msgsSize = partition.getMessages().size();
            if (offset < msgsSize && offset >= 0) {
                ArrayList<Message> playbackMsgs = new ArrayList<>();
                for (int i = offset; i < msgsSize; i++) {
                    playbackMsgs.add(partition.peekMessage(i));
                }
                consumePartitionsOffset.put(partitionId, msgsSize - 1);
                return playbackMsgs;
            } else {
                throw new ConsumerException.PlaybackOffsetIncorrectException(
                    "Playback: The offset [" + offset + "] exceed the range of the given partition ["
                    + partitionId + "]");
            }
        } else {
            throw new ConsumerException.NotAllocatdPartitionException("Playback: The consumer ["
                + consumerId + "] has not allocated to the given partition [" + partitionId + "]");
        }
    }

    @Override
    public final String toString() {
        String str = "Consumer : " + consumerId + "\n";
        for (String partitionKey : consumePartitions.keySet()) {
            PartitionImpl partition = (PartitionImpl) consumePartitions.get(partitionKey);
            str = str + "Partition - " + partition.getPartitionId()
            + " - Offset:" + consumePartitionsOffset.get(partitionKey) + "\n";
        }
        return str;
    }
}
