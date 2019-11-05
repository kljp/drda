import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class BrokerSubCheckAliveThread extends Thread{

    private Socket socket;
    private CheckAliveObject cao;

    public BrokerSubCheckAliveThread(Socket socket, CheckAliveObject cao){

        this.socket = socket;
        this.cao = cao;
    }

    @Override
    public void run(){

        int temp;

        try {
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

            while(true){

                temp = dataInputStream.readInt();

                synchronized (cao){
                    cao.setCheckAlive(temp);
                }
            }

        } catch (IOException e) {
//            e.printStackTrace();
            synchronized (cao){
                cao.setCheckAlive(0);
            }
        }
    }
}
