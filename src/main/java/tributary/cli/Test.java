/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package tributary.cli;

import tributary.api.ConsumerGroup;
import tributary.api.constants.ConsumerGroupRebalancePolicy;
import tributary.api.constants.ProducerAllocationType;
import tributary.api.exceptions.ConsumerException;
import tributary.api.exceptions.ConsumerGroupException;
import tributary.api.exceptions.ProducerException;
import tributary.api.exceptions.TopicException;

/**
 *
 * @author Michael Wang
 */
public class Test {
    //static method
    public static void main(String[] args) throws TopicException,
        ConsumerGroupException, ProducerException, ConsumerException {
        test2d();
    }

    public static void test1p() throws TopicException, ConsumerGroupException, ProducerException, ConsumerException {
        TopicOperation.createTopic("Topic1", "String");

        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        PartitionOperation.createPartition("Topic1", "P1");
        PartitionOperation.createPartition("Topic1", "P2");
        PartitionOperation.createPartition("Topic1", "P3");
        PartitionOperation.createPartition("Topic1", "P4");
        PartitionOperation.createPartition("Topic1", "P5");

        System.out.println(PartitionOperation.getPartitionInfo("Topic1").toString());

        ConsumerGroupOperation.createConsumerGroup("Topic1", "G1", ConsumerGroupRebalancePolicy.POLICY_ROUND_ROBIN);
        ConsumerGroup cg = ConsumerGroupOperation.getConsumerGroup("G1");

        System.out.println(cg.toString());

        ConsumerOperation.createConsumer("G1", "C1");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        ProducerOperation.createProducer("PD1", "String", ProducerAllocationType.MANUAL);
        System.out.println(ProducerOperation.getProducer("PD1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-1", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-2", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD2-1", "P2");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD2-2", "P2");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        System.out.println(ConsumerOperation.receiveEvent("C1", "P1").toString());
        System.out.println("After receiving event1-1");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.receiveEvent("C1", "P2").toString());
        System.out.println("After receiving event2-1");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        ConsumerOperation.createConsumer("G1", "C2");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());

        System.out.println(ConsumerOperation.receiveEvent("C1", "P1").toString());
        System.out.println("After receiving event1-2");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.receiveEvent("C2", "P2").toString());
        System.out.println("After receiving event2-2");
        System.out.println(ConsumerOperation.getConsumer("C2").toString());

        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

    }

public static void test1pr() throws TopicException, ConsumerGroupException, ProducerException, ConsumerException {
        TopicOperation.createTopic("Topic1", "String");

        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        PartitionOperation.createPartition("Topic1", "P1");
        PartitionOperation.createPartition("Topic1", "P2");
        PartitionOperation.createPartition("Topic1", "P3");
        PartitionOperation.createPartition("Topic1", "P4");
        PartitionOperation.createPartition("Topic1", "P5");

        System.out.println(PartitionOperation.getPartitionInfo("Topic1").toString());

        ConsumerGroupOperation.createConsumerGroup("Topic1", "G1", ConsumerGroupRebalancePolicy.POLICY_ROUND_ROBIN);
        ConsumerGroup cg = ConsumerGroupOperation.getConsumerGroup("G1");

        System.out.println(cg.toString());

        ConsumerOperation.createConsumer("G1", "C1");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        ProducerOperation.createProducer("PD1", "String", ProducerAllocationType.RANDOM);
        System.out.println(ProducerOperation.getProducer("PD1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-1", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-2", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-3", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-4", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-5", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        ProducerOperation.sendEvent("PD1", "Topic1", "TEST MESSAGE PD1-6", "P1");
        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        // System.out.println(ConsumerOperation.getConsumer("C1").toString());

        // System.out.println(ConsumerOperation.receiveEvent("C1", "P1").toString());
        // System.out.println("After receiving event1-1");
        // System.out.println(ConsumerOperation.getConsumer("C1").toString());

        // System.out.println(ConsumerOperation.receiveEvent("C1", "P1").toString());
        // System.out.println("After receiving event1-2");
        // System.out.println(ConsumerOperation.getConsumer("C1").toString());

        // System.out.println(ConsumerOperation.receiveEvent("C1", "P1").toString());
        // System.out.println("After receiving event1-3");
        // System.out.println(ConsumerOperation.getConsumer("C1").toString());
    }

    public static void test1() throws TopicException, ConsumerGroupException, ConsumerException {
        TopicOperation.createTopic("Topic1", "String");

        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        PartitionOperation.createPartition("Topic1", "P1");
        PartitionOperation.createPartition("Topic1", "P2");
        PartitionOperation.createPartition("Topic1", "P3");
        PartitionOperation.createPartition("Topic1", "P4");
        PartitionOperation.createPartition("Topic1", "P5");

        System.out.println(PartitionOperation.getPartitionInfo("Topic1").toString());

        ConsumerGroupOperation.createConsumerGroup("Topic1", "G1", ConsumerGroupRebalancePolicy.POLICY_ROUND_ROBIN);
        ConsumerGroup cg = ConsumerGroupOperation.getConsumerGroup("G1");

        System.out.println(cg.toString());

        ConsumerOperation.createConsumer("G1", "C1");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        ConsumerOperation.createConsumer("G1", "C2");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());

        ConsumerOperation.createConsumer("G1", "C3");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());
        System.out.println(ConsumerOperation.getConsumer("C3").toString());

        ConsumerOperation.createConsumer("G1", "C4");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());
        System.out.println(ConsumerOperation.getConsumer("C3").toString());
        System.out.println(ConsumerOperation.getConsumer("C4").toString());
    }

    public static void test1d() throws TopicException, ConsumerGroupException, ConsumerException {
        TopicOperation.createTopic("Topic1", "String");

        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        PartitionOperation.createPartition("Topic1", "P1");
        PartitionOperation.createPartition("Topic1", "P2");
        PartitionOperation.createPartition("Topic1", "P3");
        PartitionOperation.createPartition("Topic1", "P4");
        PartitionOperation.createPartition("Topic1", "P5");

        System.out.println(PartitionOperation.getPartitionInfo("Topic1").toString());

        ConsumerGroupOperation.createConsumerGroup("Topic1", "G1", ConsumerGroupRebalancePolicy.POLICY_ROUND_ROBIN);

        System.out.println(ConsumerGroupOperation.getConsumerGroup("G1").toString());

        ConsumerOperation.createConsumer("G1", "C1");
        System.out.println(ConsumerGroupOperation.getConsumerGroup("G1").toString());

        ConsumerOperation.createConsumer("G1", "C2");
        System.out.println(ConsumerGroupOperation.getConsumerGroup("G1").toString());

        ConsumerOperation.createConsumer("G1", "C3");
        System.out.println(ConsumerGroupOperation.getConsumerGroup("G1").toString());

        ConsumerOperation.createConsumer("G1", "C4");
        System.out.println(ConsumerGroupOperation.getConsumerGroup("G1").toString());

        System.out.println("Before delete");
        ConsumerOperation.deleteConsumer("C1");
        System.out.println(ConsumerGroupOperation.getConsumerGroup("G1").toString());

    }

    public static void test2() throws TopicException, ConsumerGroupException, ConsumerException {
        TopicOperation.createTopic("Topic1", "String");

        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        PartitionOperation.createPartition("Topic1", "P1");
        PartitionOperation.createPartition("Topic1", "P2");
        PartitionOperation.createPartition("Topic1", "P3");
        PartitionOperation.createPartition("Topic1", "P4");
        PartitionOperation.createPartition("Topic1", "P5");

        System.out.println(PartitionOperation.getPartitionInfo("Topic1").toString());

        ConsumerGroupOperation.createConsumerGroup("Topic1", "G1", ConsumerGroupRebalancePolicy.POLICY_RANGE);
        ConsumerGroup cg = ConsumerGroupOperation.getConsumerGroup("G1");

        System.out.println(cg.toString());

        ConsumerOperation.createConsumer("G1", "C1");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        ConsumerOperation.createConsumer("G1", "C2");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        ConsumerOperation.createConsumer("G1", "C3");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());
        System.out.println(ConsumerOperation.getConsumer("C3").toString());

        ConsumerOperation.createConsumer("G1", "C4");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());
        System.out.println(ConsumerOperation.getConsumer("C3").toString());
        System.out.println(ConsumerOperation.getConsumer("C4").toString());
    }
    public static void test2d() throws TopicException, ConsumerGroupException, ConsumerException {
        TopicOperation.createTopic("Topic1", "String");

        System.out.println(TopicOperation.getTopicInfo("Topic1").toString());

        PartitionOperation.createPartition("Topic1", "P1");
        PartitionOperation.createPartition("Topic1", "P2");
        PartitionOperation.createPartition("Topic1", "P3");
        PartitionOperation.createPartition("Topic1", "P4");
        PartitionOperation.createPartition("Topic1", "P5");

        System.out.println(PartitionOperation.getPartitionInfo("Topic1").toString());

        ConsumerGroupOperation.createConsumerGroup("Topic1", "G1", ConsumerGroupRebalancePolicy.POLICY_RANGE);
        ConsumerGroup cg = ConsumerGroupOperation.getConsumerGroup("G1");

        System.out.println(cg.toString());

        ConsumerOperation.createConsumer("G1", "C1");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());

        ConsumerOperation.createConsumer("G1", "C2");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());

        ConsumerOperation.createConsumer("G1", "C3");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());
        System.out.println(ConsumerOperation.getConsumer("C3").toString());

        ConsumerOperation.createConsumer("G1", "C4");
        System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());
        System.out.println(ConsumerOperation.getConsumer("C3").toString());
        System.out.println(ConsumerOperation.getConsumer("C4").toString());

        System.out.println("Before delete");
        ConsumerOperation.deleteConsumer("G1", "C1");
        //System.out.println(ConsumerOperation.getConsumer("C1").toString());
        System.out.println(ConsumerOperation.getConsumer("C2").toString());
        System.out.println(ConsumerOperation.getConsumer("C3").toString());
        System.out.println(ConsumerOperation.getConsumer("C4").toString());
    }
}
