import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class LoadBalancerPollThread extends Thread {

    private Socket socket = null;
    private HashMap<String, Queue<msgEPartition>> queues;
    private HashMap<Integer, String> IPMap;

    public LoadBalancerPollThread(Socket socket, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap) {

        this.socket = socket;
        this.queues = queues;
        this.IPMap = IPMap;
    }

    @Override
    public void run() {

        msgEPartition temp;

        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String remoteHostName = remoteSocketAddress.getAddress().getHostAddress();

        synchronized (queues) {
            if (!queues.containsKey(remoteHostName))
                queues.put(remoteHostName, new LinkedList<msgEPartition>());
        }

        synchronized (IPMap) {
            if (!IPMap.containsValue(remoteHostName))
                IPMap.put(IPMap.size(), remoteHostName);
        }

        try {
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            while (true) {
                synchronized (queues.get(remoteHostName)) {
                    if (!queues.get(remoteHostName).isEmpty()) {
                        temp = queues.get(remoteHostName).poll();
                        temp.writeDelimitedTo(dataOutputStream);
//                        System.out.println();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
