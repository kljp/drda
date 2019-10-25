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
    public static HashMap<String, Integer> PortList;

    public static void main(String[] args) {

        getIPAddress();
        LB_PORT = PortList.get("LB_SUB_PORT");
        SUB_PORT = PortList.get("SUB_BROKER_PORT");

        Socket socket;
        MessageWrapper messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomRangeGenerator());
        msgEPartition message;

        message = messageWrapper.buildMsgEPartition();
        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(LB_IP, LB_PORT));
            DataOutputStream dataOutputStream= new DataOutputStream(socket.getOutputStream());
            message.writeTo(dataOutputStream);
            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Queue<msgEPartition> queue = new LinkedList<msgEPartition>();
        new ClientSubPollThread(queue).start();

        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(SUB_PORT);

            while (true) {
                Socket subSocket = serverSocket.accept();
                new ClientSubRecvThread(socket, queue).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void getIPAddress(){

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
