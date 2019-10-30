import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

public class LoadBalancerMasterWorkThread extends Thread {

    private Socket socket;
    private ArrayList<Integer> wakeThread;
    private int threadId;
    private HashMap<String, LoadStatusObject> lsoMap;
    private double repDeg;

    public LoadBalancerMasterWorkThread(Socket socket, ArrayList<Integer> wakeThread, int threadId, HashMap<String, LoadStatusObject> lsoMap, double repDeg) {

        this.socket = socket;
        this.wakeThread = wakeThread;
        this.threadId = threadId;
        this.lsoMap = lsoMap;
        this.repDeg = repDeg;
    }

    @Override
    public void run() {

        int numSubscriptions;
        int accessCount;
        ArrayList<LoadStatusObject> lsos;

        DataOutputStream dataOutputStream = null;
        ObjectInputStream objectInputStream = null;

        try {
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            objectInputStream = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {

            synchronized (wakeThread) {

                if (wakeThread.get(threadId) == 1) {

                    // send request as string to the corresponding LB
                    try {
                        dataOutputStream.writeUTF("reduce");
                        dataOutputStream.flush();
                        lsos = (ArrayList<LoadStatusObject>) objectInputStream.readObject();

                        synchronized (lsoMap) {

                            for (int i = 0; i < lsos.size(); i++) {

                                if (!lsoMap.containsKey(lsos.get(i).getBROKER_IP()))
                                    lsoMap.put(lsos.get(i).getBROKER_IP(), lsos.get(i));
                                else {
                                    lsoMap.get(lsos.get(i).getBROKER_IP()).setNumSubscriptions(lsoMap.get(lsos.get(i).getBROKER_IP()).getNumSubscriptions() + lsos.get(i).getNumSubscriptions());
                                    lsoMap.get(lsos.get(i).getBROKER_IP()).setAccessCount(lsoMap.get(lsos.get(i).getBROKER_IP()).getAccessCount() + lsos.get(i).getAccessCount());
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                    wakeThread.set(threadId, 0);

                    while (true) {
                        if (wakeThread.get(threadId) == 1) {
                            try {
                                dataOutputStream.writeDouble(repDeg);
                                dataOutputStream.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            wakeThread.set(threadId, 0);
                            break;
                        }
                    }
                }
            }
        }
    }
}
