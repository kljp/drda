import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Queue;

public class ClientPubPollThread extends Thread {

    private String LB_IP;
    private int LB_PORT;
    private Queue<msgEPartition> queue;
    private int count;

    public ClientPubPollThread(String SERVER_IP, int SERVER_PORT, Queue<msgEPartition> queue, int count) {

        this.LB_IP = SERVER_IP;
        this.LB_PORT = SERVER_PORT;
        this.queue = queue;
        this.count = count;
    }

    @Override
    public void run() {

        Socket socket;
        msgEPartition temp;
        int curCount = 0;

        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(LB_IP, LB_PORT));
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            dataOutputStream.writeInt(count);
            dataOutputStream.flush();

            while (true) {
                synchronized (queue) {
                    if (!queue.isEmpty()) {
                        temp = queue.poll();
//                        System.out.println(temp);
                        temp.writeDelimitedTo(dataOutputStream);
                        dataOutputStream.flush();
                        curCount++;

                        if(curCount == count)
                            break;

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
