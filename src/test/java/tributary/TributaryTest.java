package tributary;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import tributary.api.*;
import tributary.api.exceptions.ConsumerException;
import tributary.api.exceptions.ConsumerGroupException;
import tributary.api.exceptions.ProducerException;
import tributary.api.exceptions.TopicException;
import tributary.api.exceptions.TopicException.TopicNotFoundException;
import tributary.cli.ParallelConsumer;
import tributary.cli.ParallelProducer;

import static org.junit.jupiter.api.Assertions.*;

public class TributaryTest {

    @Test
    @DisplayName("Test create topic")
    public void testCreateTopic() throws TopicNotFoundException {
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        assertEquals("topic1", topic.getTopicId());
    }

    @Test
    @DisplayName("Test create partition")
    public void testCreatePartition() throws TopicException {
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
    }

    @Test
    @DisplayName("Test delete consumer")
    public void testDeleteConsumer() throws TopicException, ConsumerException, ConsumerGroupException {
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
    @DisplayName("Test create producer")
    public void testCreateProducer() throws TopicException {
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        TributaryFactory.createProducerInstance("producerOne", "String", "Random");
        assertEquals(TributaryFactory.getProducers().size(), 1);
    }

    @Test
    @DisplayName("Test produce event")
    public void testProduceEvent() throws TopicException, ProducerException {
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createConsumerGroup("group1", "Range");
        topic.createPartition("partition");
        TributaryFactory.createProducerInstance("producerOne", "String", "Manual");
        assertEquals(TributaryFactory.getProducers().size(), 1);
        TributaryFactory.getProducerInstance("producerOne").send("topic1", "sampleEventString", "partition");
        assertEquals(topic.getPartitions().get("partition").getMessages().size(), 1);
    }

    @Test
    @DisplayName("Test consume events")
    public void testConsumeEvent() throws TopicException, ConsumerException, ConsumerGroupException, ProducerException {
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
    }

    @Test
    @DisplayName("Test show topic")
    public void testShowTopic() throws TopicException, ConsumerException, ConsumerGroupException {
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
    }

    @Test
    @DisplayName("Test parallel consume")
    public void testParallelConsume() throws TopicException,
        ConsumerException, ConsumerGroupException, ProducerException {
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
        TributaryFactory.createTopicInstance("topic1", "String");
        Topic topic = TributaryFactory.getTopicInstance("topic1");
        topic.createPartition("firstPartition");
        topic.createPartition("secondPartition");
        TributaryFactory.createProducerInstance("producerOne", "String", "Manual");
        TributaryFactory.createProducerInstance("producerTwo", "String", "Manual");
        assertEquals(TributaryFactory.getProducers().size(), 2);
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
    }


}
