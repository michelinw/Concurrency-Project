
package cli;

import tributary.api.Producer;

/**
 *
 * @author Michael Wang
 */
public class ParallelProducer extends Thread {

    private Producer producer;
    private String topicId;
    private String eventContent;
    private String partitionId;

    public ParallelProducer(Producer producer, String topicId, String eventContent, String partitionId) {
        this.producer = producer;
        this.topicId = topicId;
        this.eventContent = eventContent;
        this.partitionId = partitionId;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 10; i++) {
                System.out.println(
                    "Thread " + Thread.currentThread().getId() + ","
                    + " send event." + "topic-" + topicId + ", partition-" + partitionId
                    + ", content-" + eventContent + String.valueOf(i));
                boolean isInteger = false;
                try {
                    Integer tmp = Integer.valueOf(eventContent + String.valueOf(i));
                    isInteger = true;
                } catch (Exception ex) {
                }

                if (isInteger) {
                    Integer ieventContent = Integer.valueOf(eventContent + String.valueOf(i));
                    producer.send(topicId, ieventContent, partitionId);
                } else {
                    producer.send(topicId, eventContent + String.valueOf(i), partitionId);
                }
                sleep(1000);
            }
        } catch (Exception e) {
            // Throwing an exception
            System.out.println("Thread " + Thread.currentThread().getId() + ","
                        + "Exception is caught :" + e.getMessage());
        }
    }
}
