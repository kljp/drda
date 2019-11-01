import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.util.ArrayList;
import java.util.Queue;

public class BrokerSubPollThread extends Thread {

    private ArrayList<PubCountObject> subscriptions;
    private Queue<msgEPartition> pubQueue;
    private ArrayList<EventQueueObject> eventQueues;

    public BrokerSubPollThread(ArrayList<PubCountObject> subscriptions, Queue<msgEPartition> pubQueue, ArrayList<EventQueueObject> eventQueues) {

        this.subscriptions = subscriptions;
        this.pubQueue = pubQueue;
        this.eventQueues = eventQueues;
    }

    @Override
    public void run() {

        msgEPartition temp;
        int countExit;
        int checkEmpty;

        while (true) {

            synchronized (pubQueue) {
                if (!pubQueue.isEmpty())
                    checkEmpty = 1;

                else
                    checkEmpty = 0;
            }

            if (checkEmpty == 1) {

                synchronized (pubQueue) {
                    temp = pubQueue.poll();
                }

                synchronized (subscriptions) {

                    for (int i = 0; i < subscriptions.size(); i++) {

                        countExit = 0;

                        for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                            if (temp.getPub().getSinglePoint(j) >= subscriptions.get(i).getSubscription().getSub().getLowerBound(j)
                                    && temp.getPub().getSinglePoint(j) <= subscriptions.get(i).getSubscription().getSub().getUpperBound(j)) {

                                countExit++;
                            }

                            else
                                break;
                        }

                        if (countExit == GlobalState.NumberOfDimensions) {

                            synchronized (eventQueues) {
                                eventQueues.get(i).getEventQueue().add(temp);
                            }

                            break;
                        }
                    }
                }
            }
        }
    }
}
