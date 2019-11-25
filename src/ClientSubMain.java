import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class ClientSubMain {

    private static String LB_IP;
    private static int LB_PORT;
    private static int SUB_PORT;
    private static final String msgType = "Subscription";
    private static HashMap<String, Integer> PortList;
    private static Queue<msgEPartition> genQueue = new LinkedList<msgEPartition>();

    public static void main(String[] args) {

        getIPAddress();
        LB_PORT = PortList.get("LB_SUB_PORT");
        SUB_PORT = PortList.get("SUB_BROKER_PORT");
        int count = GlobalState.SUB_COUNT;
        MessageWrapper messageWrapper;
        msgEPartition message;

        new ClientSubGenThread(LB_IP, LB_PORT, SUB_PORT, genQueue, count).start();

        for (int i = 0; i < count; i++) {

            messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomRangeGenerator());
            message = messageWrapper.buildMsgEPartition();

            synchronized (genQueue){
                genQueue.add(message);
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void getIPAddress(){

        Socket socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(GlobalState.IPS_IP, GlobalState.IPS_CLIENT_PORT));
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            LB_IP = dataInputStream.readUTF();
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            PortList = (HashMap<String, Integer>) objectInputStream.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
