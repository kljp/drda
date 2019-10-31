import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

public class LoadBalancerSyncThread extends Thread{

    private String BROKER_IP;
    private int threadId;
    private int BROKER_PORT;
    private ArrayList<InitiatePollObject> checkPoll;
    private ArrayList<PubCountObject> sharedPcos;

    public LoadBalancerSyncThread(String BROKER_IP, int threadId, int BROKER_PORT, ArrayList<InitiatePollObject> checkPoll, ArrayList<PubCountObject> sharedPcos){

        this.BROKER_IP = BROKER_IP;
        this.threadId = threadId;
        this.BROKER_PORT = BROKER_PORT;
        this.checkPoll = checkPoll;
        this.sharedPcos = sharedPcos;
    }

    @Override
    public void run(){

        Socket socket = new Socket();
        DataOutputStream dataOutputStream = null;
        ObjectInputStream objectInputStream = null;
        PubCountObject pco;

        try {
            socket.connect(new InetSocketAddress(BROKER_IP, BROKER_PORT));
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            objectInputStream = new ObjectInputStream(socket.getInputStream());
            // poll
            while(true){
                synchronized (checkPoll.get(threadId)){

                    if(checkPoll.get(threadId).getCheck() == 1){

                        dataOutputStream.writeUTF("sync");
                        dataOutputStream.flush();
                        pco = (PubCountObject) objectInputStream.readObject();
                        pco.setBROKER_IP(BROKER_IP);

                        synchronized (sharedPcos){
                            sharedPcos.add(pco);
                        }

                        checkPoll.get(threadId).setCheck(0);
                    }
                }
            }
            // When initiated, send request and receive data
            // save it to shared data structure or any object
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
