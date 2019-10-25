import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.util.Queue;

public class ClientSubPollThread extends Thread {

    private Queue<msgEPartition> queue;

    public ClientSubPollThread(Queue<msgEPartition> queue) {

        this.queue = queue;
    }

    @Override
    public void run() {

        msgEPartition temp;

        while (true) {
            synchronized (queue) {
                if (!queue.isEmpty()) {
                    temp = queue.poll();
                    //System.out.println(temp);
                }
            }
        }
    }
}
