import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class BrokerMain {

    private static ArrayList<String> IPList;
    private static HashMap<String, Integer> PortList;
    private static Queue<msgEPartition> pubQueue = new LinkedList<msgEPartition>();
    private static Queue<msgEPartition> subQueue = new LinkedList<msgEPartition>();
    private static ArrayList<PubCountObject> subscriptions = new ArrayList<PubCountObject>();

    public static void main(String[] args) {

        IPList = null;
        PortList = null;
        getAddressList();

        for(String IP : IPList){

            new BrokerPubConnThread(IP, PortList.get("LB_BROKER_PUB_PORT"), pubQueue, "loadbalancer").start();
            new BrokerSubConnThread(IP, PortList.get("LB_BROKER_SUB_PORT"), PortList.get("SUB_BROKER_PORT"), pubQueue, subQueue, subscriptions, "loadBalancer").start();
        }

        new BrokerSyncConnThread(subscriptions, PortList.get("BROKER_LB_SYNC_PORT")).start();
    }

    private static void getAddressList(){

        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(GlobalState.IPS_IP, GlobalState.IPS_BROKER_PORT));
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            IPList = (ArrayList<String>) objectInputStream.readObject();
            PortList = (HashMap<String, Integer>) objectInputStream.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}