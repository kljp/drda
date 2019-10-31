import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class IPServerConnThread extends Thread{

    private ArrayList<String> IPList;
    private HashMap<String, Integer> PortList;
    private HashMap<String, Integer> IPSPortList;
    private int IPS_PORT;
    private ServerSocket serverSocket;

    public IPServerConnThread(ArrayList<String> IPList, HashMap<String, Integer> PortList, HashMap<String, Integer> IPSPortList, int IPS_PORT, ServerSocket serverSocket){

        this.IPList = IPList;
        this.PortList = PortList;
        this.IPSPortList = IPSPortList;
        this.IPS_PORT = IPS_PORT;
        this.serverSocket = serverSocket;
    }

    @Override
    public void run(){

        try {
            while(true){
                Socket socket = serverSocket.accept();
                new IPServerProcThread(IPList, PortList, IPSPortList, IPS_PORT, socket).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
