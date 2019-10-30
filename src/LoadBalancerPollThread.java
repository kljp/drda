import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class LoadBalancerPollThread extends Thread {

    private Socket socket = null;
    private HashMap<String, Queue<msgEPartition>> queues;
    private HashMap<Integer, String> IPMap;
    private HashMap<String, ArrayList<PubCountObject>> loadStatus;

    public LoadBalancerPollThread(Socket socket, HashMap<String, Queue<msgEPartition>> queues, HashMap<Integer, String> IPMap, HashMap<String, ArrayList<PubCountObject>> loadStatus) {

        this.socket = socket;
        this.queues = queues;
        this.IPMap = IPMap;
        this.loadStatus = loadStatus;
    }

    @Override
    public void run() {

        msgEPartition temp;

        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String remoteHostName = remoteSocketAddress.getAddress().getHostAddress();
        int remoteHostPort = remoteSocketAddress.getPort();
        double[] lowerbounds = new double[GlobalState.NumberOfDimensions];
        double[] upperbounds = new double[GlobalState.NumberOfDimensions];
        int countExit;

        synchronized (queues){
            if (!queues.containsKey(remoteHostName))
                queues.put(remoteHostName, new LinkedList<msgEPartition>());
        }

        synchronized (IPMap){
            if (!IPMap.containsValue(remoteHostName))
                IPMap.put(IPMap.size(), remoteHostName);
        }

        synchronized (loadStatus){
            if(!loadStatus.containsKey(remoteHostName))
                loadStatus.put(remoteHostName, new ArrayList<PubCountObject>());
        }

        try {
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            while (true) {
                synchronized (queues.get(remoteHostName)) {
                    if (!queues.get(remoteHostName).isEmpty()) {
                        temp = queues.get(remoteHostName).poll();

                        if(temp.getMsgType().equals("Subscription")){

                            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                                lowerbounds[i] = temp.getSub().getLowerBound(i);
                                upperbounds[i] = temp.getSub().getUpperBound(i);
                            }

                            synchronized (loadStatus){
                                loadStatus.get(remoteHostName).add(new PubCountObject(lowerbounds, upperbounds));
                            }

                            temp.writeDelimitedTo(dataOutputStream);
                        }

                        else{

                            synchronized (loadStatus){

                                for (int i = 0; i < loadStatus.get(remoteHostName).size(); i++) {

                                    countExit = 0;

                                    for (int j = 0; j < GlobalState.NumberOfDimensions; j++) {

                                        if(temp.getPub().getSinglePoint(j) >= loadStatus.get(remoteHostName).get(i).getNthLowerBound(j)
                                                && temp.getPub().getSinglePoint(j) <= loadStatus.get(remoteHostName).get(i).getNthUpperBound(j)){
                                            countExit++;
                                        }

                                        else
                                            break;
                                    }

                                    if(countExit == GlobalState.NumberOfDimensions){

                                        loadStatus.get(remoteHostName).get(i).setPubCount(loadStatus.get(remoteHostName).get(i).getPubCount() + 1);
                                        temp.writeDelimitedTo(dataOutputStream);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
