import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;

public class ClientSubGenThread extends Thread{

    private String LB_IP;
    private int LB_PORT;
    private int SUB_PORT;
    private Queue<msgEPartition> genQueue;
    private static Queue<msgEPartition> queue = new LinkedList<msgEPartition>(); // for receiving events
    private int count;

    public ClientSubGenThread(String SERVER_IP, int SERVER_PORT, int SUB_PORT, Queue<msgEPartition> genQueue, int count){

        this.LB_IP = SERVER_IP;
        this.LB_PORT = SERVER_PORT;
        this.SUB_PORT = SUB_PORT;
        this.genQueue = genQueue;
        this.count = count;
    }

    @Override
    public void run() {

        Socket socket;
        msgEPartition temp;
        int curCount = 0;

//        synchronized (queue){
            new ClientSubPollThread(queue).start();
//        }

        ServerSocket serverSocket = null;
        socket = new Socket();

        try {
            socket.connect(new InetSocketAddress(LB_IP, LB_PORT));
            serverSocket = new ServerSocket(SUB_PORT);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            dataOutputStream.writeInt(count);
            dataOutputStream.flush();

            while (true) {
                synchronized (genQueue) {
                    if (!genQueue.isEmpty()) {
                        temp = genQueue.poll();
//                        System.out.println(temp);
                        temp.writeDelimitedTo(dataOutputStream);
                        dataOutputStream.flush();

                        new ClientSubConnThread(serverSocket, queue).start();

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
