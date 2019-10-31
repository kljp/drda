import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Queue;

public class BrokerSubPollThread extends Thread {

    private String SUB_IP;
    private int SUB_PORT;
    private Queue<msgEPartition> pubQueue;
    private ArrayList<PubCountObject> subscriptions;

    public BrokerSubPollThread(String SUB_IP, int SUB_PORT, Queue<msgEPartition> pubQueue, ArrayList<PubCountObject> subscriptions) {

        this.SUB_IP = SUB_IP;
        this.SUB_PORT = SUB_PORT;
        this.pubQueue = pubQueue;
        this.subscriptions = subscriptions;
    }

    @Override
    public void run() {

        Socket socket;
        msgEPartition temp;
        int countExit;

        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(SUB_IP, SUB_PORT));
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            while (true) {
                synchronized (pubQueue) {
                    if (!pubQueue.isEmpty()) {
                        temp = pubQueue.poll();
//                        System.out.println(temp);

                        for (int i = 0; i < subscriptions.size(); i++) {

                            countExit = 0;

                            for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                                if(temp.getPub().getSinglePoint(j) >= subscriptions.get(i).getSubscription().getSub().getLowerBound(j)
                                && temp.getPub().getSinglePoint(j) <= subscriptions.get(i).getSubscription().getSub().getUpperBound(j)){

                                    countExit++;
                                }

                                else
                                    break;
                            }

                            if(countExit == GlobalState.NumberOfDimensions){

                                subscriptions.get(i).setPubCount(subscriptions.get(i).getPubCount() + 1);
                                temp.writeDelimitedTo(dataOutputStream);
                                break;
                            }
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
