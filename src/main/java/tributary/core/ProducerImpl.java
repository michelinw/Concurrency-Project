package tributary.core;

import tributary.api.TributaryFactory;
import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;
import tributary.api.Partition;
import tributary.api.Producer;
import tributary.api.constants.ProducerAllocationType;
import tributary.api.exceptions.ProducerException;
import tributary.api.exceptions.TopicException;


public class ProducerImpl implements Producer {

    /**
     *
     */
    private String producerId;

    /**
     *
     */
    private String allocationType;

    /**
     *
     */
    private String payloadType;

    /**
     *
     * @param producerId
     * @param payloadType
     * @param allocationType
     */
    public ProducerImpl(String producerId, String payloadType, String allocationType) {
        this.producerId = producerId;
        this.payloadType = payloadType;
        this.allocationType = allocationType;
    }

    /**
     *
     * @return
     */
    public String getProducerId() {
        return producerId;
    }

    /**
     *
     * @return
     */
    public String getAllocationType() {
        return allocationType;
    }

    /**
     *
     * @return
     */
    public String getPayloadType() {
        return payloadType;
    }

    /**
     *
     * @return
     */
    @Override
    public final String toString() {
        String str = "Producer : " + producerId + ", event type:"
            + payloadType + ", allocationType:" + allocationType;
        return str;
    }

    @Override
    public void send(String topicId, Object eventContent, String partitionId) throws ProducerException, TopicException {
        TopicImpl topic = (TopicImpl) TributaryFactory.getTopicInstance(topicId);
        if (topic != null) {
            // check if the event type matches the topic event type
            if (topic.getEventPayloadType().equals(this.getPayloadType())) {
                LinkedHashMap<String, Partition> partitions = topic.getPartitions();
                if (this.allocationType.equals(ProducerAllocationType.MANUAL)) {
                    if (partitionId != null && !("".equals(partitionId))) {
                        if (partitions.containsKey(partitionId)) {
                            MessageImpl msg = new MessageImpl(payloadType, partitionId, eventContent);
                            Partition partition = partitions.get(partitionId);
                            partition.addMessage(msg);
                        } else {
                            throw new ProducerException.PartitionNotFoundException("Partition Id ["
                                + partitionId + "] is not found in topic [" + topicId + "]");
                        }
                    } else {
                        throw new ProducerException.MandatoryPartitionIdMissingException(
                                "Partition Id is mandatory for manual type producer");
                    }
                } else {
                    int partitionNum = partitions.size();
                    int randomNum = ThreadLocalRandom.current().nextInt(0, partitionNum);
                    Object[] partitionKeys = partitions.keySet().toArray();
                    Partition partition = partitions.get(partitionKeys[randomNum].toString());
                    MessageImpl msg = new MessageImpl(payloadType, partitionId, eventContent);
                    partition.addMessage(msg);
                }
            } else {
                throw new ProducerException.EventTypeNotMatchToTopicException("Topic [" + topicId + "]'s event type ["
                    + topic.getEventPayloadType()
                    + "] does not match with producer's event type [" + this.getPayloadType() + "]");
            }

        } else {
            throw new ProducerException.NotExistTopicException("Topic [" + topicId + "] does not exists");
        }
    }

}
