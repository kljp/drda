import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.util.ArrayList;
import java.util.Queue;

public class BrokerSubConnThread extends Thread {

    private String LB_IP;
    private int LB_PORT;
    private int SUB_PORT;
    private Queue<msgEPartition> subQueue;
    private Queue<msgEPartition> pubQueue;
    private ArrayList<msgEPartition> subscriptions;
    private String serverType;

    public BrokerSubConnThread(String LB_IP, int LB_PORT, int SUB_PORT, Queue<msgEPartition> pubQueue, Queue<msgEPartition> subQueue, ArrayList<msgEPartition> subscriptions, String serverType) {

        this.LB_IP = LB_IP;
        this.LB_PORT = LB_PORT;
        this.SUB_PORT = SUB_PORT;
        this.subQueue = subQueue;
        this.pubQueue = pubQueue;
        this.subscriptions = subscriptions;
        this.serverType = serverType;
    }

    @Override
    public void run() {

        // If a subscription comes in, the message is parsed to get SUB_IP and SUB_PORT.
        // Then, a thread connects to the corresponding client using the address obtained above.
        // The spawned thread keeps polling whether there are new events or not.

        msgEPartition temp;
        String SUB_IP;

        new BrokerSubRecvThread(LB_IP, LB_PORT, subQueue).start();

        while (true) {

            synchronized (subQueue) {

                // check whether there are new subscriptions or not by polling
                if (!subQueue.isEmpty()) {

                    temp = subQueue.poll();
                    synchronized (subscriptions) {
                        subscriptions.add(temp);
                    }

                    // should be replaced
                    SUB_IP = temp.getIPAddress();

                    new BrokerSubPollThread(SUB_IP, SUB_PORT, pubQueue).start();
                }
            }
        }
    }
}
