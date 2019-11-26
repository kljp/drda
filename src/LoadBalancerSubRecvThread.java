import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

public class LoadBalancerSubRecvThread extends Thread {

    private Socket socket;
    private HashMap<String, Queue<msgEPartition>> queues;
    private HashMap<Integer, String> IPMap;
    private SubspaceAllocator subspaceAllocator;
    private AttributeOrderSorter attributeOrderSorter;
    private ReplicationGenerator replicationGenerator;
    private ArrayList<LoadStatusObject> lsos;
    private ReplicationDegree repDeg;
    private ArrayList<msgEPartition> subscriptions;

    public LoadBalancerSubRecvThread(Socket socket, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap,
                                     SubspaceAllocator subspaceAllocator, AttributeOrderSorter attributeOrderSorter, ReplicationGenerator replicationGenerator,
                                     ArrayList<LoadStatusObject> lsos, ReplicationDegree repDeg, ArrayList<msgEPartition> subscriptions) {

        this.socket = socket;
        this.queues = queues;
        this.IPMap = IPMap;
        this.subspaceAllocator = subspaceAllocator;
        this.attributeOrderSorter = attributeOrderSorter;
        this.replicationGenerator = replicationGenerator;
        this.lsos = lsos;
        this.repDeg = repDeg;
        this.subscriptions = subscriptions;
    }

    @Override
    public void run() {

        msgEPartition temp;
        msgEPartition tempMsgGlobal = null;
        String tempStr;
        DataInputStream dataInputStream;
        InetSocketAddress remoteSocketAddress;
        String remoteHostName;
        msgEPartition[] messages;
        int count = 0;

        try {
            dataInputStream = new DataInputStream(socket.getInputStream());
            count = dataInputStream.readInt();

            for (int j = 0; j < count; j++) {

                temp = msgEPartition.parseDelimitedFrom(dataInputStream);
                remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
                remoteHostName = remoteSocketAddress.getAddress().getHostAddress();
                temp = attributeOrderSorter.sortAttributeOrder(temp);
                temp = subspaceAllocator.allocateSubspace(temp);
                temp = replicationGenerator.setIPAddress(temp, remoteHostName);
                messages = replicationGenerator.generateReplicates(temp);
                System.out.println(messages[0].getSub().getId());

                if(messages.length > 1) {
                    synchronized (IPMap) {
                        messages = replicationGenerator.preventDuplicates(messages, IPMap);
                    }
                }

                if(GlobalState.DRDA_MODE.equals("ON")){

                    synchronized (repDeg){

                        if(messages.length > repDeg.getRepDegInt()){

                            if(lsos.size() > 0){
                                synchronized (IPMap){
                                    messages = replicationGenerator.applyReplicationDegree(messages, IPMap, lsos, repDeg.getRepDegInt(), 1);
                                    tempMsgGlobal = replicationGenerator.setGlobalSub(messages, IPMap);
                                }
                            }

                            else{
                                synchronized (IPMap){
                                    messages = replicationGenerator.applyReplicationDegree(messages, IPMap, lsos, repDeg.getRepDegInt(), 0);
                                    tempMsgGlobal = replicationGenerator.setGlobalSub(messages, IPMap);
                                }
                            }

                            synchronized (subscriptions){
                                subscriptions.add(tempMsgGlobal);
                            }
                        }
                    }
                }

                else if(GlobalState.DRDA_MODE.equals("SEMI")){

                    if(messages.length > 3){

                        if(lsos.size() > 0){
                            synchronized (IPMap){
                                messages = replicationGenerator.applyReplicationDegree(messages, IPMap, lsos, 3, 1);
                                tempMsgGlobal = replicationGenerator.setGlobalSub(messages, IPMap);
                            }
                        }

                        else{
                            synchronized (IPMap){
                                messages = replicationGenerator.applyReplicationDegree(messages, IPMap, lsos, 3, 0);
                                tempMsgGlobal = replicationGenerator.setGlobalSub(messages, IPMap);
                            }
                        }

                        synchronized (subscriptions){
                            subscriptions.add(tempMsgGlobal);
                        }
                    }
                }

                for (int i = 0; i < messages.length; i++) {

                    synchronized (IPMap){
                        tempStr = IPMap.get(Math.abs(MurmurHash.hash32(messages[i].getSubspaceForward())) % IPMap.size());
                    }

                    synchronized (queues.get(tempStr)) {
                        queues.get(tempStr).add(messages[i]);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
