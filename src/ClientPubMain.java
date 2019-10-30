import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class ClientPubMain {

    private static String LB_IP;
    private static int LB_PORT;
    private static final String msgType = "Publication";
    private static HashMap<String, Integer> PortList;
    private static Queue<msgEPartition> queue = new LinkedList<msgEPartition>();

    public static void main(String[] args) {

        getIPAddress();
        LB_PORT = PortList.get("LB_PUB_PORT");

        MessageWrapper messageWrapper;
        msgEPartition message;
        int count = GlobalState.PUB_COUNT;

        new ClientPubPollThread(LB_IP, LB_PORT, queue, count).start();

        for (int i = 0; i < count; i++) { // Publish i messages

            messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomValueGenerator());
            message = messageWrapper.buildMsgEPartition();

            synchronized (queue){
                queue.add(message);
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
