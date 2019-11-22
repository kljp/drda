import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;

public class ClientSubConnThread extends Thread{

    private ServerSocket serverSocket;
    private Queue<msgEPartition> queue;

    public ClientSubConnThread(ServerSocket serverSocket, Queue<msgEPartition> queue){

        this.serverSocket = serverSocket;
        this.queue = queue;
    }

    @Override
    public void run(){

        try {
            while(true){
                Socket subSocket = serverSocket.accept();
                new ClientSubRecvThread(subSocket, queue).start();

                if(GlobalState.UNSUB_MODE.equals("ON"))
                    new ClientSubPingAliveThread(subSocket).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
