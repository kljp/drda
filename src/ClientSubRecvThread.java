import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Queue;

public class ClientSubRecvThread extends Thread{

    private Socket socket;
    private Queue<msgEPartition> queue;

    public ClientSubRecvThread(Socket socket, Queue<msgEPartition> queue){

        this.socket = socket;
        this.queue = queue;
    }

    @Override
    public void run(){

        msgEPartition temp;
        DataInputStream dataInputStream = null;

        try {
            dataInputStream = new DataInputStream(socket.getInputStream());

            while (true) {

                temp = msgEPartition.parseDelimitedFrom(dataInputStream);

                synchronized (queue) {
                    queue.add(temp);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
