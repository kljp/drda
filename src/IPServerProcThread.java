import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class IPServerProcThread extends Thread{

    private ArrayList<String> IPList;
    private HashMap<String, Integer> PortList;
    private HashMap<String, Integer> IPSPortList;
    private int IPS_PORT;
    private Socket socket;

    public IPServerProcThread(ArrayList<String> IPList,HashMap<String, Integer> PortList, HashMap<String, Integer> IPSPortList, int IPS_PORT, Socket socket){

        this.IPList = IPList;
        this.PortList = PortList;
        this.IPSPortList = IPSPortList;
        this.IPS_PORT = IPS_PORT;
        this.socket = socket;
    }

    @Override
    public void run(){

        if(IPS_PORT == IPSPortList.get("IPS_LB_PORT"))
            IPServerLBProc();
        else if(IPS_PORT == IPSPortList.get("IPS_BROKER_PORT"))
            IPServerBrokerProc();
        else // IPS_PORT == IPSPortList.get("IPS_CLIENT_PORT")
            IPServerClientProc();
    }

    public void IPServerLBProc(){

        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String remoteHostName = remoteSocketAddress.getAddress().getHostAddress();

        synchronized (IPList){
            IPList.add(remoteHostName);
        }

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(PortList);
            objectOutputStream.flush();
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void IPServerBrokerProc(){

        ArrayList<String> temp;

        synchronized (IPList){
            temp = IPList;
        }

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(temp);
            objectOutputStream.flush();
            objectOutputStream.writeObject(PortList);
            objectOutputStream.flush();
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void IPServerClientProc(){

        String temp;
        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String remoteHostName = remoteSocketAddress.getAddress().getHostAddress();

        synchronized (IPList){
            temp = IPList.get(MurmurHash.hash32(remoteHostName) % IPList.size());
        }

        try {
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeUTF(temp);
//            dataOutputStream.flush();
//            dataOutputStream.close();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(PortList);
            objectOutputStream.flush();
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
