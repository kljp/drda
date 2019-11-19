import com.EPartition.GlobalSyncObject.SyncObject;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class LoadBalancerCommThread extends Thread {

    private int LBIdentifier;
    private String LBMaster;
    private int LB_PORT;
    private int BROKER_PORT;
    private static int curMaster;
    private static ArrayList<Integer> wakeThread = new ArrayList<Integer>();
    private static ArrayList<ArrayList<String>> BrokerList = new ArrayList<ArrayList<String>>();
    private HashMap<Integer, String> IPMap;
    private static ArrayList<InitiatePollObject> checkPoll = new ArrayList<InitiatePollObject>();
    private static ArrayList<LoadStatusObject> sharedLsos = new ArrayList<LoadStatusObject>();
    private ArrayList<LoadStatusObject> lsos;
    private static ArrayList<LoadStatusObject> tempLsos = new ArrayList<LoadStatusObject>();
    private ReplicationDegree repDeg;
    private static int curSync = 0;


    public LoadBalancerCommThread(int LBIdentifier, String LBMaster, int LB_PORT, int BROKER_PORT, HashMap<Integer, String> IPMap, ArrayList<LoadStatusObject> lsos, ReplicationDegree repDeg) {

        this.LBIdentifier = LBIdentifier;
        this.LBMaster = LBMaster;
        this.LB_PORT = LB_PORT;
        this.BROKER_PORT = BROKER_PORT;
        this.curMaster = 0;
        this.IPMap = IPMap;
        this.lsos = lsos;
        this.repDeg = repDeg;
    }

    @Override
    public void run() { // Crush is inevitable if some load balancers come in after curMaster is already changed. Thus, it should be modified in the future.

        while (true)
            electMaster();
    }

    private void electMaster() {

        if (LBIdentifier == curMaster) { // Only Master LB comes in.

            System.out.println("master");

            new LoadBalancerMasterNotfThread(wakeThread, BrokerList, IPMap, repDeg, lsos, tempLsos, BROKER_PORT, curSync).start();

            try {
                ServerSocket serverSocket = new ServerSocket(LB_PORT);

                while (true) {
                    Socket socket = serverSocket.accept();

                    synchronized (wakeThread) {
                        wakeThread.add(0);
                    }

                    new LoadBalancerMasterWorkThread(socket, wakeThread, wakeThread.size() - 1, BrokerList, IPMap, repDeg, tempLsos, lsos, curSync).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else { // LBs come in except the master.

            Socket cliSocket;

//            synchronized (checkPoll){
//                checkPoll = new ArrayList<InitiatePollObject>();
//            }

            if (curMaster > 0) {

                try {
                    cliSocket = new Socket();
                    cliSocket.connect(new InetSocketAddress(GlobalState.IPS_IP, GlobalState.IPS_LB_PORT));
                    DataOutputStream dataOutputStream = new DataOutputStream(cliSocket.getOutputStream());
                    dataOutputStream.writeUTF("elect");
                    dataOutputStream.flush();
                    dataOutputStream.writeInt(curMaster);
                    dataOutputStream.flush();

                    DataInputStream dataInputStream = new DataInputStream(cliSocket.getInputStream());
                    LBMaster = dataInputStream.readUTF();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                cliSocket = new Socket();
                cliSocket.connect(new InetSocketAddress(LBMaster, LB_PORT)); // LBMaster should be the latest IP Address.
                // In while loop, wait for request (by dataInputStream.readUTF())
                // Then, receive request as string from the worker thread of master LB
                DataInputStream dataInputStream = new DataInputStream(cliSocket.getInputStream());
                DataOutputStream dataOutputStream = new DataOutputStream(cliSocket.getOutputStream());
                ObjectInputStream objectInputStream = new ObjectInputStream(cliSocket.getInputStream());
                ArrayList<String> brokers;

                String tempStr;

                while (true) {

                    tempStr = dataInputStream.readUTF();

                    if (tempStr.equals("connect")) {

                        brokers = (ArrayList<String>) objectInputStream.readObject();

                        synchronized (checkPoll) {
                            for (int i = 0; i < brokers.size(); i++) {
                                checkPoll.add(new InitiatePollObject(0));
                            }
                        }

                        synchronized (checkPoll) {
                            for (int i = 0; i < brokers.size(); i++) {
                                new LoadBalancerSyncThread(brokers.get(i), i, BROKER_PORT, checkPoll, sharedLsos).start();
                            }
                        }

                        applyGlobalSync(dataInputStream);

                        synchronized (lsos){
                            if (!lsos.isEmpty()) {

                                synchronized (repDeg){
                                    System.out.println(repDeg.getRepDegDouble() + " " + repDeg.getRepDegInt());
                                }

                                for (int i = 0; i < lsos.size(); i++)
                                    System.out.println(lsos.get(i).getBROKER_IP() + " " + lsos.get(i).getNumSubscriptions() + " " + lsos.get(i).getAccessCount());
                                System.out.println();
                            }
                        }


                    } else if (tempStr.equals("reduce")) {

                        synchronized (checkPoll) {
                            for (int i = 0; i < checkPoll.size(); i++) {
                                checkPoll.get(i).setCheck(1);
                            }
                        }

                        while (true) {
                            synchronized (sharedLsos) {
                                if (sharedLsos.size() == checkPoll.size()) {
                                    sendLsos(dataOutputStream);
                                    sharedLsos.clear();
                                    break;
                                }
                            }
                        }

                        applyGlobalSync(dataInputStream);

                        System.out.println("curSync = " + curSync);

                        synchronized (repDeg){
                            System.out.println(repDeg.getRepDegDouble() + " " + repDeg.getRepDegInt());
                        }

                        synchronized (lsos){
                            if (!lsos.isEmpty()) {
                                for (int i = 0; i < lsos.size(); i++)
                                    System.out.println(lsos.get(i).getBROKER_IP() + " " + lsos.get(i).getNumSubscriptions() + " " + lsos.get(i).getAccessCount());
                            }
                        }

                        System.out.println();
                    }
                }
            } catch (IOException e) {
//                e.printStackTrace();
                curMaster++;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void applyGlobalSync(DataInputStream dataInputStream){

        SyncObject syncObject;

        try {
            syncObject = SyncObject.parseDelimitedFrom(dataInputStream);

            synchronized (repDeg){
                repDeg.setRepDegDouble(syncObject.getRepDeg().getRepDegDouble());
                repDeg.setRepDegInt(syncObject.getRepDeg().getRepDegInt());
            }

            synchronized (lsos){

                lsos.clear();

                for (int i = 0; i < syncObject.getLsoList().size(); i++) {
                    LoadStatusObject lso = new LoadStatusObject();
                    lso.setBROKER_IP(syncObject.getLso(i).getBROKERIP());
                    lso.setNumSubscriptions(syncObject.getLso(i).getNumSubscriptions());
                    lso.setAccessCount(syncObject.getLso(i).getAccessCount());
                    lsos.add(lso);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendLsos(DataOutputStream dataOutputStream){

        SyncObject syncObject;
        SyncObject.Builder syncObjectBuilder;
        SyncObject.LoadStatusObject.Builder lsob;

        try {

            syncObjectBuilder = SyncObject.newBuilder();

            synchronized (lsos) {
                for (int i = 0; i < sharedLsos.size(); i++) {
                    lsob = SyncObject.LoadStatusObject.newBuilder();
                    lsob.setBROKERIP(sharedLsos.get(i).getBROKER_IP());
                    lsob.setNumSubscriptions(sharedLsos.get(i).getNumSubscriptions());
                    lsob.setAccessCount(sharedLsos.get(i).getAccessCount());
                    syncObjectBuilder.addLso(lsob);
                }
            }

            syncObject = syncObjectBuilder.build();
            syncObject.writeDelimitedTo(dataOutputStream);
            dataOutputStream.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
