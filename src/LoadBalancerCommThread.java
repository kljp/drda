import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
    private int curMaster;
    private static ArrayList<Integer> wakeThread = new ArrayList<Integer>();
    private HashMap<String, ArrayList<PubCountObject>> loadStatus;
    private static HashMap<String, LoadStatusObject> lsoMap = new HashMap<String, LoadStatusObject>();
    private static double repDeg = GlobalState.REP_DEG_INIT;

    public LoadBalancerCommThread(int LBIdentifier, String LBMaster, int LB_PORT, HashMap<String, ArrayList<PubCountObject>> loadStatus) {

        this.LBIdentifier = LBIdentifier;
        this.LBMaster = LBMaster;
        this.LB_PORT = LB_PORT;
        this.curMaster = 0;
        this.loadStatus = loadStatus;
    }

    @Override
    public void run() { // Crush is inevitable if some load balancers come in after curMaster is already changed. Thus, it should be modified in the future.

        while(true)
            electMaster(curMaster);
    }

    private void electMaster(int curMaster){

        if (LBIdentifier == curMaster) { // Only Master LB comes in.

            new LoadBalancerMasterNotfThread(wakeThread, lsoMap, repDeg).start();

            try {
                ServerSocket serverSocket = new ServerSocket(LB_PORT);

                while (true) {
                    Socket socket = serverSocket.accept();

                    synchronized (wakeThread){
                        wakeThread.add(0);
                    }

                    new LoadBalancerMasterWorkThread(socket, wakeThread, wakeThread.size() - 1, lsoMap, repDeg).start();
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
                String tempStr;
                String key;
                Iterator<String> keys;
                ArrayList<LoadStatusObject> lsos;
                LoadStatusObject lso;

                while(true){

                    tempStr = dataInputStream.readUTF();

                    if(tempStr.equals("reduce")){

                        lsos = new ArrayList<LoadStatusObject>();

                        synchronized (loadStatus){

                            if(!loadStatus.isEmpty()){

                                keys = loadStatus.keySet().iterator();

                                while(keys.hasNext()){

                                    key = keys.next();
                                    lso = new LoadStatusObject(key);
                                    lso.setNumSubscriptions(loadStatus.get(key).size());
                                    lso.setAccessCount(0);

                                    for (int i = 0; i < loadStatus.get(key).size(); i++)
                                        lso.setAccessCount(lso.getAccessCount() + loadStatus.get(key).get(i).getPubCount());

                                    lsos.add(lso);
                                }
                            }
                        }

                        objectOutputStream.writeObject(lsos);
                        objectOutputStream.flush();
                        repDeg = dataInputStream.readDouble();
                    }
                }
            } catch (IOException e) {
//                e.printStackTrace();
                curMaster++;
            }
        }
    }
}