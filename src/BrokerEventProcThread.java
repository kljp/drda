import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Queue;

public class BrokerEventProcThread extends Thread {

    private String SUB_IP;
    private int SUB_PORT;
    private ArrayList<EventQueueObject> eventQueues;
    private ArrayList<PubCountObject> subscriptions;
    private int seqThread;

    public BrokerEventProcThread(String SUB_IP, int SUB_PORT, ArrayList<EventQueueObject> eventQueues, ArrayList<PubCountObject> subscriptions, int seqThread) {

        this.SUB_IP = SUB_IP;
        this.SUB_PORT = SUB_PORT;
        this.eventQueues = eventQueues;
        this.subscriptions = subscriptions;
        this.seqThread = seqThread;
    }

    @Override
    public void run() {

        Socket socket;
        msgEPartition temp = null;
        CheckAliveObject cao;
        int checkAlive;
        DataOutputStream dataOutputStream = null;

        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(SUB_IP, SUB_PORT));
            cao = new CheckAliveObject(1);
            dataOutputStream = new DataOutputStream(socket.getOutputStream());

            new BrokerSubCheckAliveThread(socket, cao).start();

            while (true) {

                synchronized (cao) {
                    if (cao.getCheckAlive() == 1)
                        checkAlive = 1;
                    else
                        checkAlive = 0;
                }


                if (checkAlive == 1) {
                    try {
                        for (int i = 0; i < eventQueues.size(); i++) {

                            if (eventQueues.get(i).getSeqThread() == seqThread) {

                                if (!eventQueues.get(i).getEventQueue().isEmpty()) {

                                    synchronized (eventQueues.get(i)) {
                                        temp = eventQueues.get(i).getEventQueue().poll();
                                    }

                                    if (temp != null)
                                        temp.writeDelimitedTo(dataOutputStream);

                                    else
                                        break;

                                    dataOutputStream.flush();

                                    synchronized (subscriptions) {
                                        subscriptions.get(i).setPubCount(subscriptions.get(i).getPubCount() + 1);
                                    }
                                }

                                break;
                            }
                        }
                    } catch (IOException e) {
                        terminateThread();
                        return;
                    }
                } else {

                    terminateThread();
                    return;
                }
            }

        } catch (IOException e) {
            terminateThread();
            return;
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                terminateThread();
                return;
            }
        }
    }

    public void terminateThread() {

        synchronized (eventQueues) {

            for (int i = 0; i < eventQueues.size(); i++) {

                if (eventQueues.get(i).getSeqThread() == seqThread) {

                    eventQueues.remove(i);

                    synchronized (subscriptions) {
                        subscriptions.remove(i);
                    }

                    return;
                }
            }
        }
    }
}