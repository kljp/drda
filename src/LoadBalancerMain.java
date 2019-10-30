import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

public class LoadBalancerMain {

    private static HashMap<String, Integer> PortList;
    private static HashMap<String, Queue<msgEPartition>> subQueues = new HashMap<String, Queue<msgEPartition>>();
    private static HashMap<String, Queue<msgEPartition>> pubQueues = new HashMap<String, Queue<msgEPartition>>();
    private static HashMap<String, ArrayList<PubCountObject>> loadStatus = new HashMap<String, ArrayList<PubCountObject>>();
    private static HashMap<Integer, String> IPMap = new HashMap<Integer, String>();
    private static int LBIdentifier;
    private static String LBMaster;

    public static void main(String[] args) {

        SubspaceAllocator subspaceAllocator = new SubspaceAllocator();
        AttributeOrderSorter attributeOrderSorter = new AttributeOrderSorter(new AttributeOrder());
        ReplicationGenerator replicationGenerator = new ReplicationGenerator();

        PortList = null;
        registerIPAddress();

        new LoadBalancerCommThread(LBIdentifier, LBMaster, PortList.get("LB_LB_PORT"), loadStatus);

        new LoadBalancerConnThread(PortList.get("LB_SUB_PORT"), "client", "Subscription", subQueues, IPMap, subspaceAllocator, attributeOrderSorter, replicationGenerator).start(); // 5002: connection between LB and clients (sub)
        new LoadBalancerConnThread(PortList.get("LB_PUB_PORT"), "client", "Publication", pubQueues, IPMap, subspaceAllocator, attributeOrderSorter, replicationGenerator).start(); // 5003: connection between LB and clients (pub)
        new LoadBalancerConnThread(PortList.get("LB_BROKER_PUB_PORT"), "broker", "Publication", pubQueues, IPMap, loadStatus).start(); // 5004: connection between LB and brokers (pub)
        new LoadBalancerConnThread(PortList.get("LB_BROKER_SUB_PORT"), "broker", "Subscription", subQueues, IPMap, loadStatus).start(); // 5005: connection between LB and brokers (sub)
    }

    private static void registerIPAddress(){

        Socket socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(GlobalState.IPS_IP, GlobalState.IPS_LB_PORT));
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeUTF("conn");
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            PortList = (HashMap<String, Integer>) objectInputStream.readObject();
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            LBIdentifier = dataInputStream.readInt();
            LBMaster = dataInputStream.readUTF();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
