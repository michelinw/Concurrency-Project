
package cli;

import tributary.api.Consumer;

/**
 *
 * @author Michael Wang
 */
public class ParallelConsumer extends Thread {

    private Consumer consumer;
    private String partitionId;

    public ParallelConsumer(Consumer consumer, String partitionId) {
        this.consumer = consumer;
        this.partitionId = partitionId;
    }

    @Override
    public void run() {
        try {
            // Thread will wait until receive 10 events then exits
            for (int i = 0; i < 10; i++) {
                System.out.println(
                    "Thread " + Thread.currentThread().getId() + "," + " receive "
                    + consumer.receive(partitionId).toString());
                sleep(1000);
            }
        } catch (Exception e) {
            System.out.println("Thread " + Thread.currentThread().getId() + ","
                + " Exception is caught :" + e.getMessage());
        }
    }
}
