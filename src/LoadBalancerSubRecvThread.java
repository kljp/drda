import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Queue;

public class LoadBalancerSubRecvThread extends Thread {

    private Socket socket;
    private HashMap<String, Queue<msgEPartition>> queues;
    private HashMap<Integer, String> IPMap;
    private SubspaceAllocator subspaceAllocator;
    private AttributeOrderSorter attributeOrderSorter;
    private ReplicationGenerator replicationGenerator;

    public LoadBalancerSubRecvThread(Socket socket, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap,
                                     SubspaceAllocator subspaceAllocator, AttributeOrderSorter attributeOrderSorter, ReplicationGenerator replicationGenerator) {

        this.socket = socket;
        this.queues = queues;
        this.IPMap = IPMap;
        this.subspaceAllocator = subspaceAllocator;
        this.attributeOrderSorter = attributeOrderSorter;
        this.replicationGenerator = replicationGenerator;
    }

    @Override
    public void run() {

        msgEPartition temp;
        String tempStr;
        DataInputStream dataInputStream;
        InetSocketAddress remoteSocketAddress;
        String remoteHostName;
        msgEPartition[] messages;

        try {
            dataInputStream = new DataInputStream(socket.getInputStream());

            temp = msgEPartition.parseDelimitedFrom(dataInputStream);
            remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
            remoteHostName = remoteSocketAddress.getAddress().getHostAddress();
            temp = attributeOrderSorter.sortAttributeOrder(temp);
            temp = subspaceAllocator.allocateSubspace(temp);
            temp = replicationGenerator.setIPAddress(temp, remoteHostName);
//            System.out.println(temp);

            messages = replicationGenerator.generateReplicates(temp);

            if(messages.length > 1)
                messages = replicationGenerator.preventDuplicates(messages, IPMap);

            for (int i = 0; i < messages.length; i++) {

                synchronized (IPMap){
                    tempStr = IPMap.get(MurmurHash.hash32(messages[i].getSubspaceForward()) % IPMap.size());
                }

                synchronized (queues.get(tempStr)) {
                    queues.get(tempStr).add(messages[i]);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
