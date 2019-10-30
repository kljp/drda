import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

public class LoadBalancerConnThread extends Thread {

    private int LB_PORT;
    private String clientType;
    private String msgType;
    private HashMap<String, Queue<msgEPartition>> queues;
    private HashMap<Integer, String> IPMap;
    private SubspaceAllocator subspaceAllocator;
    private AttributeOrderSorter attributeOrderSorter;
    private ReplicationGenerator replicationGenerator;
    private HashMap<String, ArrayList<PubCountObject>> loadStatus;

    public LoadBalancerConnThread(int LB_PORT, String clientType, String msgType, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap, HashMap<String, ArrayList<PubCountObject>> loadStatus) {  // for brokers

        this.LB_PORT = LB_PORT;
        this.clientType = clientType;
        this.msgType = msgType;
        this.queues = queues;
        this.IPMap = IPMap;
        this.loadStatus = loadStatus;
    }

    public LoadBalancerConnThread(int LB_PORT, String clientType, String msgType, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap, // for clients
                                  SubspaceAllocator subspaceAllocator, AttributeOrderSorter attributeOrderSorter, ReplicationGenerator replicationGenerator) {

        this.LB_PORT = LB_PORT;
        this.clientType = clientType;
        this.msgType = msgType;
        this.queues = queues;
        this.IPMap = IPMap;
        this.subspaceAllocator = subspaceAllocator;
        this.attributeOrderSorter = attributeOrderSorter;
        this.replicationGenerator = replicationGenerator;
    }

    @Override
    public void run() {

        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(LB_PORT);

            while (true) {
                Socket socket = serverSocket.accept();

                if (clientType.equals("client")) {

                    if (msgType.equals("Subscription"))
                        new LoadBalancerSubRecvThread(socket, queues, IPMap, subspaceAllocator, attributeOrderSorter, replicationGenerator).start(); // each branch will be modified whether the incoming message is subscription or not.
                    else
                        new LoadBalancerPubRecvThread(socket, queues, IPMap, subspaceAllocator, attributeOrderSorter, replicationGenerator).start();
                } else {
                    if(msgType.equals("Subscription"))
                        new LoadBalancerPollThread(socket, queues, IPMap, loadStatus).start();
                    else
                        new LoadBalancerPollThread(socket, queues, IPMap, loadStatus).start();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
