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
    private ReplicationDegree repDeg;
    private ArrayList<LoadStatusObject> tempLsos;

    public LoadBalancerMasterWorkThread(Socket socket, ArrayList<Integer> wakeThread, int threadId, ArrayList<ArrayList<String>> BrokerList, HashMap<Integer, String> IPMap, ReplicationDegree repDeg, ArrayList<LoadStatusObject> tempLsos) {

        this.socket = socket;
        this.wakeThread = wakeThread;
        this.threadId = threadId;
        this.BrokerList = BrokerList;
        this.IPMap = IPMap;
        this.repDeg = repDeg;
        this.tempLsos = tempLsos;
    }

    @Override
    public void run() {

        DataOutputStream dataOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        ObjectInputStream objectInputStream = null;
        int checkFirst = 0;
        ArrayList<LoadStatusObject> tempLso;

        try {
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectInputStream = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {


            if (wakeThread.get(threadId) == 1) {

                // send request as string to the corresponding LB
                try {
                    if (checkFirst == 0) { // only come in when initiated

                        dataOutputStream.writeUTF("connect");
                        dataOutputStream.flush();
                        objectOutputStream.writeObject(BrokerList.get(threadId));
                        objectOutputStream.flush();

                        checkFirst = 1;
                    } else {
                        System.out.println("6");
                        dataOutputStream.writeUTF("reduce");
                        dataOutputStream.flush();

                        tempLso = (ArrayList<LoadStatusObject>) objectInputStream.readObject();

                        synchronized (tempLsos) {
                            tempLsos.addAll(tempLso);
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                synchronized (wakeThread){
                    wakeThread.set(threadId, 0);
                }

                while (true) {
                    System.out.println("7");
                    if (wakeThread.get(threadId) == 1) {
//                        synchronized (repDeg) {
                        System.out.println("8");
                            try {
                                objectOutputStream.writeObject(repDeg);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
//                        }

                        synchronized (wakeThread){
                            wakeThread.set(threadId, 0);
                        }

                        break;
                    }
                }
            }

        }
    }
}
