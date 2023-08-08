package tributary;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import cli.ParallelConsumer;
import cli.ParallelProducer;
import tributary.api.*;
import tributary.api.exceptions.ConsumerException;
import tributary.api.exceptions.ConsumerGroupException;
import tributary.api.exceptions.ProducerException;
import tributary.api.exceptions.TopicException;
import tributary.api.exceptions.TopicException.TopicNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

public class TributaryTest {

    @Test
    @DisplayName("Test create topic")
    public void testCreateTopic() throws TopicNotFoundException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        assertEquals("topic1", topic.getTopicId());
        assertDoesNotThrow(() -> topic.toString());
    }

    @Test
    @DisplayName("Test can't create topic")
    public void testFailedCreateTopic() throws TopicNotFoundException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        assertThrows(TopicNotFoundException.class, () -> TributaryFactory.getTopicInstance("topic2"));
    }

    @Test
    @DisplayName("Test Delete Topic")
    public void testDeleteTopic() throws TopicNotFoundException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        assertEquals(TributaryFactory.getTopics().size(), 1);
        TributaryFactory.deleteTopicInstance("topic1");
        assertEquals(TributaryFactory.getTopics().size(), 0);
    }

    @Test
    @DisplayName("Test create partition")
    public void testCreatePartition() throws TopicException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        assertEquals("topic1", topic.getTopicId());
        assertEquals(topic.getPartitionListing().size(), 0);
        topic.createPartition("firstPartition");
        assertEquals(topic.getPartitionListing().size(), 1);
    }

    @Test
    @DisplayName("Test create consumer group")
    public void testCreateConsumerGroup() throws TopicNotFoundException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        assertEquals(topic.getConsumerGroups().size(), 0);
        topic.createConsumerGroup("firstGroup", "Range");
        assertEquals(topic.getConsumerGroups().size(), 1);
        topic.createConsumerGroup("secondGroup", "RoundRobin");
        assertEquals(topic.getConsumerGroups().size(), 2);
    }

    @Test
    @DisplayName("Test create consumers")
    public void testCreateConsumer() throws TopicException, ConsumerException, ConsumerGroupException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        assertEquals(group1.getConsumers().size(), 0);
        assertEquals(group2.getConsumers().size(), 0);
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().get("firstConsumer").getConsumerId(), "firstConsumer");
        assertEquals(group1.getConsumers().get("firstConsumer").getConsumerGroupId(), "group1");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        assertDoesNotThrow(() -> group1.getConsumers().get("firstConsumer").toString());
    }

    @Test
    @DisplayName("Test can't create consumer")
    public void testFailedCreateConsumer() throws TopicException, ConsumerException, ConsumerGroupException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        assertEquals(group1.getConsumers().size(), 0);
        assertThrows(ConsumerGroupException.class, () -> group1.addConsumer("firstConsumer"));
    }

    @Test
    @DisplayName("Test delete consumer")
    public void testDeleteConsumer() throws TopicException, ConsumerException, ConsumerGroupException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        assertEquals(group1.getConsumers().size(), 0);
        assertEquals(group2.getConsumers().size(), 0);
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        group1.deleteConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 0);
        group2.deleteConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 0);
    }

    @Test
    @DisplayName("Test can't delete consumer")
    public void testFailedDeleteConsumer() throws ConsumerException, TopicException, ConsumerGroupException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        assertEquals(group1.getConsumers().size(), 0);
        assertEquals(group2.getConsumers().size(), 0);
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        group1.deleteConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 0);
        group2.deleteConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 0);
        assertThrows(ConsumerGroupException.class, () -> group2.deleteConsumer("thirdConsumer"));
        assertDoesNotThrow(() -> group1.toString());
        assertEquals(group1.getConsumerGroupId(), "group1");
        assertEquals(group1.getRebalancePolicy(), "Range");
    }

    @Test
    @DisplayName("Change rebalance policy")
    public void testChangeRebalancePolicy() throws ConsumerException, TopicException, ConsumerGroupException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        assertEquals(group1.getConsumers().size(), 0);
        assertEquals(group2.getConsumers().size(), 0);
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        assertDoesNotThrow(() -> group1.setRebalancePolicy("RoundRobin"));
        assertDoesNotThrow(() -> group2.setRebalancePolicy("RoundRobin"));
    }

    @Test
    @DisplayName("Test create and delete producer")
    public void testCreateDeleteProducer() throws TopicException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        TributaryFactory.createProducerInstance("producerOne", "String", "Random");
        assertEquals(TributaryFactory.getProducers().size(), 1);
        assertEquals(TributaryFactory.getProducerInstance("producerOne").getProducerId(), "producerOne");
        assertEquals(TributaryFactory.getProducerInstance("producerOne").getAllocationType(), "Random");
        TributaryFactory.deleteProducerInstance("producerOne");
        assertEquals(TributaryFactory.getProducers().size(), 0);
    }

    @Test
    @DisplayName("Test produce event string")
    public void testProduceEventString() throws TopicException, ProducerException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        topic.createPartition("partition");
        TributaryFactory.createProducerInstance("producerOne", "String", "Manual");
        assertEquals(TributaryFactory.getProducers().size(), 1);
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString", "partition");
        assertEquals(topic.getPartitions().get("partition").getMessages().size(), 1);
        assertThrows(ProducerException.class, () ->
            TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString", "partition2"));
        assertThrows(ProducerException.class, () ->
            TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString", ""));
        assertThrows(TopicException.class, () ->
            TributaryFactory.getProducerInstance("producerOne").send("", "sampleEventString", "partition"));
        assertDoesNotThrow(() -> TributaryFactory.getProducerInstance("producerOne").toString());
        assertDoesNotThrow(() -> topic.getPartitions().get("partition").getMessages().get(0).getId());
        assertDoesNotThrow(() -> topic.getPartitions().get("partition").getMessages().get(0).getDatetime());
        assertEquals(topic.getPartitions().get("partition").getMessages().get(0).getPayloadType(), "String");
        assertDoesNotThrow(() -> topic.getPartitions().get("partition").getMessages().get(0).getKey());
        TributaryFactory.createProducerInstance("producerTwo", "String", "Random");
        assertEquals(TributaryFactory.getProducers().size(), 2);
        assertDoesNotThrow(() ->
            TributaryFactory.getProducerInstance("producerTwo").send("topic1", "sampleEventString", "partition"));
    }

    @Test
    @DisplayName("Test consume events strings")
    public void testConsumeEventString() throws TopicException,
        ConsumerException, ConsumerGroupException, ProducerException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        topic.createPartition("thirdPartition");
        group1.addConsumer("firstConsumer");
        group1.addConsumer("secondConsumer");
        assertEquals(group1.getConsumers().size(), 2);
        TributaryFactory.createProducerInstance("producerOne", "String", "Manual");
        assertEquals(TributaryFactory.getProducers().size(), 1);
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString", "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString2", "secondPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString3", "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString4", "secondPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString4", "thirdPartition");
        group1.getConsumers().get("firstConsumer").receive("firstPartition");
        group1.getConsumers().get("firstConsumer").receive("secondPartition");
        group1.getConsumers().get("secondConsumer").receive("thirdPartition");
        assertEquals(group1.getConsumers().get("firstConsumer").getConsumePartitionsOffset().size(), 2);
        assertEquals(group1.getConsumers().get("secondConsumer").getConsumePartitionsOffset().size(), 1);
        assertThrows(ConsumerException.class, () ->
            group1.getConsumers().get("secondConsumer").receive("firstPartition"));
    }

    @Test
    @DisplayName("Test produce event integer and failure")
    public void testProduceEventInt() throws TopicException, ProducerException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topicOne", "Integer");
        Topic topic = TributaryFactory.getTopicInstance("topicOne");
        topic.createConsumerGroup("group1", "Range");
        topic.createPartition("partition1");
        TributaryFactory.createProducerInstance("producer1", "Integer", "Manual");
        TributaryFactory.getProducerInstance("producer1").send("topicOne", 1, "partition1");
        assertEquals(topic.getPartitions().get("partition1").getMessages().size(), 1);
        TributaryFactory.createProducerInstance("producer2", "String", "Manual");
        assertThrows(ProducerException.class, () ->
            TributaryFactory.getProducerInstance("producer2").send("topicOne", "sampleEvent", "partition1"));
    }

    @Test
    @DisplayName("Test consume events integers")
    public void testConsumeEventInt() throws TopicException,
        ConsumerException, ConsumerGroupException, ProducerException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "Integer");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        TributaryFactory.createProducerInstance("producerOne", "Integer", "Manual");
        assertEquals(TributaryFactory.getProducers().size(), 1);
        TributaryFactory.getProducerInstance("producerOne").send("topic1", 1, "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", 2, "secondPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", 3, "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", 4, "secondPartition");
        group1.getConsumers().get("firstConsumer").receive("firstPartition");
        group1.getConsumers().get("firstConsumer").receive("secondPartition");
        group2.getConsumers().get("secondConsumer").receive("firstPartition");
        group2.getConsumers().get("secondConsumer").receive("secondPartition");
        assertEquals(group1.getConsumers().get("firstConsumer").getConsumePartitionsOffset().size(), 2);
        assertEquals(group2.getConsumers().get("secondConsumer").getConsumePartitionsOffset().size(), 2);
        assertDoesNotThrow(() -> topic.getPartitions().get("firstPartition").getMessages().get(0).getContent());
    }

    @Test
    @DisplayName("Test show topic")
    public void testShowTopic() throws TopicException, ConsumerException, ConsumerGroupException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        assertDoesNotThrow(() -> TributaryFactory.getTopicInstance("topic1"));
        assertDoesNotThrow(() -> TributaryFactory.getTopicInstance("topic1").toString());
    }

    @Test
    @DisplayName("Test parallel consume")
    public void testParallelConsume() throws TopicException,
        ConsumerException, ConsumerGroupException, ProducerException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        TributaryFactory.createProducerInstance("producerOne", "String", "Manual");
        assertEquals(TributaryFactory.getProducers().size(), 1);
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString", "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString2", "secondPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString3", "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString4", "secondPartition");
        Consumer consumer1 = group1.getConsumers().get("firstConsumer");
        Consumer consumer2 = group2.getConsumers().get("secondConsumer");
        ParallelConsumer parallelConsumer1 = new ParallelConsumer(consumer1, "firstPartition");
        ParallelConsumer parallelConsumer2 = new ParallelConsumer(consumer2, "secondPartition");

        Thread thread1 = new Thread(parallelConsumer1);
        Thread thread2 = new Thread(parallelConsumer2);

        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            System.out.println("Interrupted Exception");
        }

        assertEquals(group1.getConsumers().get("firstConsumer").getConsumePartitionsOffset().size(), 2);
        assertEquals(group2.getConsumers().get("secondConsumer").getConsumePartitionsOffset().size(), 2);
    }

    @Test
    @DisplayName("Test parallel produce")
    public void testParallelProduce() throws TopicException,
        ConsumerException, ConsumerGroupException, ProducerException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        TributaryFactory.createProducerInstance("producerOne", "String", "Manual");
        TributaryFactory.createProducerInstance("producerTwo", "String", "Manual");
        Producer producer1 = TributaryFactory.getProducerInstance("producerOne");
        Producer producer2 = TributaryFactory.getProducerInstance("producerTwo");
        ParallelProducer parallelProducer1 =
            new ParallelProducer(producer1, "topic1", "sampleEventContent", "firstPartition");
        ParallelProducer parallelProducer2 =
            new ParallelProducer(producer2, "topic1", "sampleEventContent1", "secondPartition");

        Thread thread1 = new Thread(parallelProducer1);
        Thread thread2 = new Thread(parallelProducer2);

        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            System.out.println("Interrupted Exception");
        }

        assertEquals(topic.getPartitions().get("firstPartition").getMessages().size(), 10);
        assertEquals(topic.getPartitions().get("secondPartition").getMessages().size(), 10);
    }

    @Test
    @DisplayName("Test playback")
    public void testPlayback() throws ConsumerException, ProducerException, TopicException, ConsumerGroupException {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        ConsumerGroup group1 = topic.getConsumerGroupId("group1");
        topic.createConsumerGroup("group2", "RoundRobin");
        ConsumerGroup group2 = topic.getConsumerGroupId("group2");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        group1.addConsumer("firstConsumer");
        assertEquals(group1.getConsumers().size(), 1);
        group2.addConsumer("secondConsumer");
        assertEquals(group2.getConsumers().size(), 1);
        TributaryFactory.createProducerInstance("producerOne", "String", "Manual");
        assertEquals(TributaryFactory.getProducers().size(), 1);
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString", "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString2", "secondPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString3", "firstPartition");
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString4", "secondPartition");
        group1.getConsumers().get("firstConsumer").receive("firstPartition");
        group1.getConsumers().get("firstConsumer").receive("secondPartition");
        group2.getConsumers().get("secondConsumer").receive("firstPartition");
        group2.getConsumers().get("secondConsumer").receive("secondPartition");
        assertEquals(group1.getConsumers().get("firstConsumer").getConsumePartitionsOffset().size(), 2);
        assertEquals(group2.getConsumers().get("secondConsumer").getConsumePartitionsOffset().size(), 2);

        assertDoesNotThrow(() -> group1.getConsumers().get("firstConsumer").playback("firstPartition", 0));
        assertDoesNotThrow(() -> group1.getConsumers().get("firstConsumer").playback("secondPartition", 0));
        assertDoesNotThrow(() -> group2.getConsumers().get("secondConsumer").playback("firstPartition", 0));
        assertDoesNotThrow(() -> group2.getConsumers().get("secondConsumer").playback("secondPartition", 0));

        assertEquals(group1.getConsumers().get("firstConsumer").getConsumePartitionsOffset().size(), 2);
        assertEquals(group2.getConsumers().get("secondConsumer").getConsumePartitionsOffset().size(), 2);

        assertThrows(ConsumerException.class, () ->
            group1.getConsumers().get("firstConsumer").playback("firstPartition", 3));
        assertThrows(ConsumerException.class, () ->
        group1.getConsumers().get("firstConsumer").playback("fakePartition", 0));
    }

    @Test
    @DisplayName("Test producers")
    public void testProducers() {
        TributaryFactory.clear();
        TributaryFactory.createTopicInstance("topic1", "String");
        System.out.println(TributaryFactory.getProducers().size());
    }
}
