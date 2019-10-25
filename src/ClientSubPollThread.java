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

            temp = null;

            synchronized (queue) {
                if (!queue.isEmpty())
                    temp = queue.poll();
            }

            if(temp != null)
                System.out.println(temp);
        }
    }
}
