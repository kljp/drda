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

            new LoadBalancerMasterNotfThread(wakeThread, BrokerList, IPMap, repDeg, lsos, tempLsos, BROKER_PORT).start();

            try {
                ServerSocket serverSocket = new ServerSocket(LB_PORT);

                while (true) {
                    Socket socket = serverSocket.accept();

                    synchronized (wakeThread) {
                        wakeThread.add(0);
                    }

                    new LoadBalancerMasterWorkThread(socket, wakeThread, wakeThread.size() - 1, BrokerList, IPMap, repDeg, tempLsos).start();
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
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(cliSocket.getOutputStream());
                ObjectInputStream objectInputStream = new ObjectInputStream(cliSocket.getInputStream());
                String tempStr;
                ArrayList<String> brokers;

                while (true) {

                    System.out.println("1");
                    tempStr = dataInputStream.readUTF();
                    System.out.println(tempStr);
                    System.out.println("2");

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
                    } else if (tempStr.equals("reduce")) {
                        System.out.println("A");
                        synchronized (checkPoll) {
                            for (int i = 0; i < checkPoll.size(); i++) {
                                checkPoll.get(i).setCheck(1);
                            }
                        }
                        System.out.println("B");
                        while (true) {
                            synchronized (sharedLsos) {
                                if (sharedLsos.size() == checkPoll.size()) {
                                    objectOutputStream.writeObject(sharedLsos);
                                    objectOutputStream.flush();
                                    sharedLsos = null;
                                    break;
                                }
                            }
                        }
                        System.out.println("C");
                        synchronized (repDeg) {
                            repDeg = (ReplicationDegree) objectInputStream.readObject();
                        }
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
}
