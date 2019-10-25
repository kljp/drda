import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Queue;

public class LoadBalancerMain {

    public static HashMap<String, Integer> PortList;
    public static HashMap<String, Queue<msgEPartition>> subQueues = new HashMap<String, Queue<msgEPartition>>();
    public static HashMap<String, Queue<msgEPartition>> pubQueues = new HashMap<String, Queue<msgEPartition>>();
    public static HashMap<Integer, String> IPMap = new HashMap<Integer, String>();

    public static void main(String[] args) {

        SubspaceAllocator subspaceAllocator = new SubspaceAllocator();
        AttributeOrderSorter attributeOrderSorter = new AttributeOrderSorter(new AttributeOrder());
        ReplicationGenerator replicationGenerator = new ReplicationGenerator();

        PortList = null;
        registerIPAddress();

        new LoadBalancerConnThread(PortList.get("LB_SUB_PORT"), "client", "subscription", subQueues, IPMap, subspaceAllocator, attributeOrderSorter, replicationGenerator).start(); // 5002: connection between LB and clients (sub)
        new LoadBalancerConnThread(PortList.get("LB_PUB_PORT"), "client", "publication", pubQueues, IPMap, subspaceAllocator, attributeOrderSorter, replicationGenerator).start(); // 5003: connection between LB and clients (pub)
        new LoadBalancerConnThread(PortList.get("LB_BROKER_PUB_PORT"), "broker", "publication", pubQueues, IPMap).start(); // 5004: connection between LB and brokers (pub)
        new LoadBalancerConnThread(PortList.get("LB_BROKER_SUB_PORT"), "broker", "subscription", subQueues, IPMap).start(); // 5005: connection between LB and brokers (sub)
    }

    public static void registerIPAddress(){

        Socket socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(GlobalState.IPS_IP, GlobalState.IPS_LB_PORT));
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            PortList = (HashMap<String, Integer>) objectInputStream.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
