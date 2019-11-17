import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

public class LoadBalancerMasterSyncProcThread extends Thread{

    private String BROKER_IP;
    private int BROKER_PORT;
    private ArrayList<Integer> wakeThread;
    private int threadId;
    private ArrayList<LoadStatusObject> tempLsos;

    public LoadBalancerMasterSyncProcThread(String BROKER_IP, int BROKER_PORT, ArrayList<Integer> wakeThread, int threadId, ArrayList<LoadStatusObject> tempLsos){

        this.BROKER_IP = BROKER_IP;
        this.BROKER_PORT = BROKER_PORT;
        this.wakeThread = wakeThread;
        this.threadId = threadId;
        this.tempLsos = tempLsos;
    }

    @Override
    public void run(){

        Socket socket = new Socket();
        DataOutputStream dataOutputStream = null;
        ObjectInputStream objectInputStream = null;
        LoadStatusObject lso;

        try {
            socket.connect(new InetSocketAddress(BROKER_IP, BROKER_PORT));
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            objectInputStream = new ObjectInputStream(socket.getInputStream());

            while(true){

                synchronized (wakeThread) {

                    if (wakeThread.get(threadId) == 1) {

                        dataOutputStream.writeInt(1);
                        dataOutputStream.flush();
                        lso = (LoadStatusObject) objectInputStream.readObject();
                        lso.setBROKER_IP(BROKER_IP);

                        synchronized (tempLsos){
                            tempLsos.add(lso);
                        }

                        wakeThread.set(threadId, 0);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
