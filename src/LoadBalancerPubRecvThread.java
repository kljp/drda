import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

public class LoadBalancerPubRecvThread extends Thread {

    private Socket socket;
    private HashMap<String, Queue<msgEPartition>> queues;
    private HashMap<Integer, String> IPMap;
    private SubspaceAllocator subspaceAllocator;
    private AttributeOrderSorter attributeOrderSorter;
    private ReplicationGenerator replicationGenerator;
    private ArrayList<LoadStatusObject> lsos;
    private ArrayList<msgEPartition> subscriptions;

    public LoadBalancerPubRecvThread(Socket socket, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap,
                                     SubspaceAllocator subspaceAllocator, AttributeOrderSorter attributeOrderSorter, ReplicationGenerator replicationGenerator, ArrayList<LoadStatusObject> lsos, ArrayList<msgEPartition> subscriptions) {

        this.socket = socket;
        this.queues = queues;
        this.IPMap = IPMap;
        this.subspaceAllocator = subspaceAllocator;
        this.attributeOrderSorter = attributeOrderSorter;
        this.replicationGenerator = replicationGenerator;
        this.lsos = lsos;
        this.subscriptions = subscriptions;
    }

    @Override
    public void run() {

        msgEPartition temp;
        String tempStr;
        int tempInt;
        DataInputStream dataInputStream;
        InetSocketAddress remoteSocketAddress;
        String remoteHostName;
        msgEPartition[] messages;
        int count = 0;
        int countExit;

        try {
            dataInputStream = new DataInputStream(socket.getInputStream());
            count = dataInputStream.readInt();

            for (int k = 0; k < count; k++) {

                temp = msgEPartition.parseDelimitedFrom(dataInputStream);
                remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
                remoteHostName = remoteSocketAddress.getAddress().getHostAddress();
                temp = attributeOrderSorter.sortAttributeOrder(temp);
                temp = subspaceAllocator.allocateSubspace(temp);
                temp = replicationGenerator.setIPAddress(temp, remoteHostName);
//                System.out.println(temp);

                if(GlobalState.DRDA_MODE.equals("ON") || GlobalState.DRDA_MODE.equals("SEMI")){

                    LoadStatusObject[] lsoArray;
                    int[] loads;
                    LoadStatusObject tempLso;

                    synchronized (lsos){
                        lsoArray = lsos.toArray(new LoadStatusObject[lsos.size()]);
                    }

                    loads = new int[lsoArray.length];

                    if(GlobalState.LOAD_OPTION.equals("SUB")){
                        for (int i = 0; i < lsoArray.length; i++)
                            loads[i] = lsoArray[i].getNumSubscriptions();
                    }
                    else if(GlobalState.LOAD_OPTION.equals("AC")){
                        for (int i = 0; i < lsoArray.length; i++)
                            loads[i] = lsoArray[i].getAccessCount();
                    }
                    else if(GlobalState.LOAD_OPTION.equals("ALL")){
                        for (int i = 0; i < lsoArray.length; i++)
                            loads[i] = lsoArray[i].getNumSubscriptions() * lsoArray[i].getAccessCount();
                    }

                    // currently, it is implemented by the least-loaded broker selection with considering both the number of subscription and the access count.
                    // In the future, more options should be additionally implemented: 1. random, 2. probabilistic, 3. only considering the number of subscription + every strategy.
                    for (int i = 0; i < loads.length; i++) {

                        for (int j = 0; j < loads.length - i - 1; j++) {

                            if(loads[j] > loads[j + 1]){

                                tempInt = loads[j + 1];
                                loads[j + 1] = loads[j];
                                loads[j] = tempInt;

                                tempLso = lsoArray[j + 1];
                                lsoArray[j + 1] = lsoArray[j];
                                lsoArray[j] = tempLso;
                            }
                        }
                    }

                    synchronized (subscriptions) {

                        for (int i = 0; i < subscriptions.size(); i++) {

                            countExit = 0;
                            System.out.println(subscriptions.get(i));
                            for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                                if (temp.getPub().getSinglePoint(j) >= subscriptions.get(i).getSub().getLowerBound(j)
                                        && temp.getPub().getSinglePoint(j) <= subscriptions.get(i).getSub().getUpperBound(j)) {

                                    countExit++;
                                }

                                else
                                    break;
                            }

                            if (countExit == GlobalState.NumberOfDimensions) {

                                String[] brokers = subscriptions.get(i).getBrokersList().toArray(new String[subscriptions.get(i).getBrokersList().size()]);
                                int[] indexes = new int[brokers.length];

                                for (int j = 0; j < indexes.length; j++) {
                                    for (int l = 0; l < lsoArray.length; l++) {
                                        if(brokers[j].equals(lsoArray[l].getBROKER_IP())){
                                            indexes[j] = l;
                                            break;
                                        }
                                    }
                                }

                                int tempInt2;
                                String tempStr2;

                                for (int a = 0; a < indexes.length; a++) {

                                    for (int b = 0; b < indexes.length - a - 1; b++) {

                                        if(indexes[b] > indexes[b + 1]){

                                            tempInt2 = indexes[b + 1];
                                            indexes[b + 1] = indexes[b];
                                            indexes[b] = tempInt2;

                                            tempStr2 = brokers[b + 1];
                                            brokers[b + 1] = brokers[b];
                                            brokers[b] = tempStr2;
                                        }
                                    }
                                }

                                synchronized (queues.get(brokers[0])){
                                    queues.get(brokers[0]).add(temp);
                                }

                                break;
                            }
                        }

                        // retention should be added.
                    }
                }

                else{

                    synchronized (IPMap){
                        tempStr = IPMap.get(Math.abs(MurmurHash.hash32(temp.getSubspace(0))) % IPMap.size()); // same with -> tempStr = IPMap.get(MurmurHash.hash32(temp.getSubspace((int) Math.random() % temp.getSubspaceList().size())) % IPMap.size());
                    }

                    synchronized (queues.get(tempStr)) {
                        queues.get(tempStr).add(temp);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
