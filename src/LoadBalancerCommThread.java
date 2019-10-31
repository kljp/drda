import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class LoadBalancerCommThread extends Thread {

    private int LBIdentifier;
    private String LBMaster;
    private int LB_PORT;
    private int BROKER_PORT;
    private int curMaster;
    private static ArrayList<Integer> wakeThread = new ArrayList<Integer>();
    private static ArrayList<ArrayList<String>> BrokerList;
    private HashMap<Integer, String> IPMap;

    public LoadBalancerCommThread(int LBIdentifier, String LBMaster, int LB_PORT, int BROKER_PORT, HashMap<Integer, String> IPMap) {

        this.LBIdentifier = LBIdentifier;
        this.LBMaster = LBMaster;
        this.LB_PORT = LB_PORT;
        this.BROKER_PORT = BROKER_PORT;
        this.curMaster = 0;
        this.IPMap = IPMap;
    }

    @Override
    public void run() { // Crush is inevitable if some load balancers come in after curMaster is already changed. Thus, it should be modified in the future.

        while(true)
            electMaster(curMaster);
    }

    private void electMaster(int curMaster){

        if (LBIdentifier == curMaster) { // Only Master LB comes in.

            new LoadBalancerMasterNotfThread(wakeThread, BrokerList, IPMap).start();

            try {
                ServerSocket serverSocket = new ServerSocket(LB_PORT);

                while (true) {
                    Socket socket = serverSocket.accept();

                    synchronized (wakeThread){
                        wakeThread.add(0);
                    }

                    new LoadBalancerMasterWorkThread(socket, wakeThread, wakeThread.size() - 1, BrokerList, IPMap).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        else{ // LBs come in except the master.

            Socket cliSocket;

            if(curMaster > 0){

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

                while(true){

                    tempStr = dataInputStream.readUTF();

                    if(tempStr.equals("connect")) {

                        brokers = (ArrayList<String>) objectInputStream.readObject();

                        for (int i = 0; i < brokers.size(); i++) {

                            new LoadBalancerSyncThread(brokers.get(i), BROKER_PORT).start();
                        }
                    }

                    else if(tempStr.equals("reduce")){

                        // initiate polling thread (LoadBalancerSyncThread)
                        // After that, wait for data using loop
                        // give it to master LB's worker thread which is connected to this LB's thread.
                        // the worker thread save it to shared data structure or any object
                        // master thread calculate replication degree and update it to shared object
                        // each worker thread send this object to the corresponding LB's thread
                        // Each LB save it and set the replication degree as new value
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
