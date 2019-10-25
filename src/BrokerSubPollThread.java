import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Queue;

public class BrokerSubPollThread extends Thread {

    private String SUB_IP;
    private int SUB_PORT;
    private Queue<msgEPartition> pubQueue;

    public BrokerSubPollThread(String SUB_IP, int SUB_PORT, Queue<msgEPartition> pubQueue) {

        this.SUB_IP = SUB_IP;
        this.SUB_PORT = SUB_PORT;
        this.pubQueue = pubQueue;
    }

    @Override
    public void run() {

        Socket socket;
        msgEPartition temp;

        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(SUB_IP, SUB_PORT));
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            while (true) {
                synchronized (pubQueue) {
                    if (!pubQueue.isEmpty()) {
                        temp = pubQueue.poll();
                        //System.out.println(temp);
                        temp.writeTo(dataOutputStream);
                    }
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
