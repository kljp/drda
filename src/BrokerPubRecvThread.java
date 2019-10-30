import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Queue;

public class BrokerPubRecvThread extends Thread{

    private String LB_IP;
    private int LB_PORT;
    private Queue<msgEPartition> queue;

    public BrokerPubRecvThread(String LB_IP, int LB_PORT, Queue<msgEPartition> queue){

        this.LB_IP = LB_IP;
        this.LB_PORT = LB_PORT;
        this.queue = queue;
    }

    @Override
    public void run(){

        Socket socket;
        msgEPartition temp;

        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(LB_IP, LB_PORT));
            DataInputStream dataInputStream= new DataInputStream(socket.getInputStream());

            while (true) {

                temp = msgEPartition.parseDelimitedFrom(dataInputStream);
//                System.out.println(temp);

                synchronized (queue) {
                    queue.add(temp);
                }
            }

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
    }
}
