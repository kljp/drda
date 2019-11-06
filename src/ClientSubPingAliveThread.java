import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientSubPingAliveThread extends Thread{

    private Socket socket;

    public ClientSubPingAliveThread(Socket socket){

        this.socket = socket;
    }

    @Override
    public void run(){

        DataOutputStream dataOutputStream = null;
        double before;
        double after;
        double elapsed;

        try {
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            before = System.currentTimeMillis();

            while(true){
                after = System.currentTimeMillis();
                elapsed = (after - before) / 1000.0;
                if(elapsed > 5){
                    dataOutputStream.writeInt(1);
                    before = System.currentTimeMillis();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
