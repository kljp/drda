import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Queue;

public class LoadBalancerPubRecvThread extends Thread {

    private Socket socket;
    private HashMap<String, Queue<msgEPartition>> queues;
    private HashMap<Integer, String> IPMap;
    private SubspaceAllocator subspaceAllocator;
    private AttributeOrderSorter attributeOrderSorter;
    private ReplicationGenerator replicationGenerator;

    public LoadBalancerPubRecvThread(Socket socket, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap,
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
        int count = 0;

        try {
            dataInputStream = new DataInputStream(socket.getInputStream());
            count = dataInputStream.readInt();

            for (int i = 0; i < count; i++) {

                temp = msgEPartition.parseDelimitedFrom(dataInputStream);
                remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
                remoteHostName = remoteSocketAddress.getAddress().getHostAddress();
                temp = attributeOrderSorter.sortAttributeOrder(temp);
                temp = subspaceAllocator.allocateSubspace(temp);
                temp = replicationGenerator.setIPAddress(temp, remoteHostName);
//                System.out.println(temp);

                synchronized (IPMap){
                    tempStr = IPMap.get(Math.abs(MurmurHash.hash32(temp.getSubspace(0))) % IPMap.size()); // same with -> tempStr = IPMap.get(MurmurHash.hash32(temp.getSubspace((int) Math.random() % temp.getSubspaceList().size())) % IPMap.size());
                }

                synchronized (queues.get(tempStr)) {
                    queues.get(tempStr).add(temp);

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
