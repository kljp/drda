import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

public class LoadBalancerMasterWorkThread extends Thread{

    private Socket socket;
    private ArrayList<Integer> wakeThread;
    private int threadId;

    public LoadBalancerMasterWorkThread(Socket socket, ArrayList<Integer> wakeThread, int threadId){

        this.socket = socket;
        this.wakeThread = wakeThread;
        this.threadId = threadId;
    }

    @Override
    public void run(){

        DataOutputStream dataOutputStream;

        while(true){

            synchronized (wakeThread){

                if(wakeThread.get(threadId) == 1){

                    wakeThread.set(threadId, 0);

                    // send request as string to the corresponding LB
                    try {
                        dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        dataOutputStream.writeUTF("reduce");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
