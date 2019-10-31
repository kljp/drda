import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

public class LoadBalancerMasterWorkThread extends Thread {

    private Socket socket;
    private ArrayList<Integer> wakeThread;
    private int threadId;
    private ArrayList<ArrayList<String>> BrokerList;
    private HashMap<Integer, String> IPMap;

    public LoadBalancerMasterWorkThread(Socket socket, ArrayList<Integer> wakeThread, int threadId, ArrayList<ArrayList<String>> BrokerList, HashMap<Integer, String> IPMap) {

        this.socket = socket;
        this.wakeThread = wakeThread;
        this.threadId = threadId;
        this.BrokerList = BrokerList;
        this.IPMap = IPMap;
    }

    @Override
    public void run() {

        DataOutputStream dataOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        int checkFirst = 0;

        try {
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {

            synchronized (wakeThread) {

                if (wakeThread.get(threadId) == 1) {

                    // send request as string to the corresponding LB
                    try {
                        if(checkFirst == 0){ // only come in when initiated

                            dataOutputStream.writeUTF("connect");
                            dataOutputStream.flush();
                            objectOutputStream.writeObject(BrokerList.get(threadId));
                            objectOutputStream.flush();

                            checkFirst = 1;
                        }

                        else{

                            dataOutputStream.writeUTF("reduce");
                            dataOutputStream.flush();

                            //do something which is written in LoadBalancerCommThread code
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    wakeThread.set(threadId, 0);
                }
            }
        }
    }
}
