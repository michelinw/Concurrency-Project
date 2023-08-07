
package tributary.cli;

import java.util.ArrayList;
import java.util.Scanner;
import tributary.api.Consumer;
import tributary.api.Message;
import tributary.api.Producer;
import tributary.api.constants.ProducerAllocationType;
import tributary.api.exceptions.ConsumerException;
import tributary.api.exceptions.ConsumerGroupException;
import tributary.api.exceptions.ProducerException;
import tributary.api.exceptions.TopicException;

/**
 *
 * @author Michael Wang
 */
public class TributaryCLI {

    public static void main(String[] args) {
        System.out.println("Welcome to Tributary Cli. Please enter your command: ");
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print(">>");
            String line = input.nextLine().trim();
            String[] params = line.split("[\\s\\(\\),]+");
            if (line.startsWith("create topic") && params.length == 4) {
                String id = params[2];
                String type = params[3];
                TopicOperation.createTopic(id, type);
                try {
                    System.out.println(TopicOperation.getTopicInfo(id).toString());
                } catch (TopicException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("create partition") && params.length == 4) {
                String topic = params[2];
                String id = params[3];
                try {
                    PartitionOperation.createPartition(topic, id);
                    System.out.println(PartitionOperation.getPartitionInfo(topic).toString());
                } catch (TopicException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("create consumer group") && params.length == 6) {
                String id = params[3];
                String topic = params[4];
                String rebalancing = params[5];
                try {
                    ConsumerGroupOperation.createConsumerGroup(topic, id, rebalancing);
                    System.out.println(ConsumerGroupOperation.getConsumerGroup(id).toString());
                } catch (TopicException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("create consumer") && params.length == 4) {
                String group = params[2];
                String id = params[3];
                try {
                    ConsumerOperation.createConsumer(group, id);
                    System.out.println(ConsumerOperation.getConsumer(id).toString());
                } catch (ConsumerGroupException | TopicException | ConsumerException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("delete consumer") && params.length == 3) {
                String id = params[2];
                try {
                    Consumer consumer = ConsumerOperation.getConsumer(id);
                    String consumerGroupId = consumer.getConsumerGroupId();
                    ConsumerOperation.deleteConsumer(id);
                    System.out.println("Consumer ["
                        + id + "] delete successfully. The current consumer group information:");
                    System.out.println(ConsumerGroupOperation.getConsumerGroup(consumerGroupId).toString());
                } catch (ConsumerGroupException | TopicException | ConsumerException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("create producer") && params.length == 5) {
                String id = params[2];
                String type = params[3];
                String allocation = params[4];
                if (allocation.equals(ProducerAllocationType.MANUAL)
                    || allocation.equals(ProducerAllocationType.RANDOM)) {
                    ProducerOperation.createProducer(id, type, allocation);
                    System.out.println(ProducerOperation.getProducer(id).toString());
                } else {
                    System.out.println("Error: Wrong allocation type-" + allocation);
                }
            } else if (line.startsWith("produce event") && params.length == 6) {
                String producer = params[2];
                String topic = params[3];
                String event = params[4];
                String partition = params[5];
                try {
                    ProducerOperation.sendEvent(producer, topic, event, partition);
                    System.out.println(TopicOperation.getTopicInfo(topic).toString());
                } catch (ProducerException | TopicException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("consume event") && params.length == 4) {
                String consumer = params[2];
                String partition = params[3];
                try {
                    System.out.println(ConsumerOperation.receiveEvent(consumer, partition));
                    System.out.println(ConsumerOperation.getConsumer(consumer).toString());
                } catch (ConsumerException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("consume events") && params.length == 5) {
                String consumer = params[2];
                String partition = params[3];
                int numberEvent = Integer.parseInt(params[4]);
                try {
                    for (int i = 0; i < numberEvent; i++) {
                        System.out.println("consume envent count no." + i);
                        System.out.println(ConsumerOperation.receiveEvent(consumer, partition));
                        System.out.println(ConsumerOperation.getConsumer(consumer).toString());
                    }
                } catch (ConsumerException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("show topic") && params.length == 3) {
                String topic = params[2];
                try {
                    System.out.println(TopicOperation.getTopicInfo(topic).toString());
                } catch (TopicException ex) {
                    System.out.println(ex.getMessage());
                }
            } else if (line.startsWith("show consumer group") && params.length == 4) {
                String group = params[3];
                System.out.println(ConsumerGroupOperation.getConsumerGroup(group).toString());
            } else if (line.startsWith("parallel produce") && params.length >= 5) {
                for (int i = 0; i < (params.length - 2) / 3; i++) {
                    String producerId = params[i * 3 + 2];
                    Producer producer = ProducerOperation.getProducer(producerId);
                    String topic = params[i * 3 + 3];
                    String event = params[i * 3 + 4];
                    ParallelProducer parallelProducer = new ParallelProducer(producer,
                        topic, event + String.valueOf(i), null);
                    parallelProducer.start();
                }
            } else if (line.startsWith("parallel consume") && params.length >= 4) {
                for (int i = 0; i < (params.length - 2) / 2; i++) {
                    String consumerId = params[i * 2 + 2];
                    Consumer consumer = ConsumerOperation.getConsumer(consumerId);
                    String partition = params[i * 2 + 3];
                    ParallelConsumer parallelConsumer = new ParallelConsumer(consumer, partition);
                    parallelConsumer.start();
                }
            } else if (line.startsWith("set consumer group rebalancing") && params.length == 6) {
                String group = params[4];
                String rebalancing = params[5];
                rebalance(group, rebalancing);
            } else if (line.startsWith("playback") && params.length == 4) {
                String consumer = params[1];
                String partition = params[2];
                int offset = Integer.parseInt(params[3]);
                runPlayback(consumer, partition, offset);
            } else if (line.startsWith("bye")) {
                input.close();
                break;
            } else System.out.println("Unknown Command!!!");
        }
    }

    private static void rebalance(String group, String rebalancing) {
        try {
            ConsumerGroupOperation.setConsumerGroupRebalancing(group, rebalancing);
            System.out.println(ConsumerGroupOperation.getConsumerGroup(group).toString());
        } catch (TopicException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void runPlayback(String consumer, String partition, int offset) {
        try {
            ArrayList<Message> messages = ConsumerOperation.playbackEvent(consumer, partition, offset);
            for (int i = 0; i < messages.size(); i++) {
                System.out.println("playback envent count no." + i);
                System.out.println(messages.get(i).toString());
            }
        } catch (ConsumerException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
