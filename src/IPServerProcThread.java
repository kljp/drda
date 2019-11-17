import javax.xml.crypto.Data;
import java.io.*;
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

    public IPServerProcThread(ArrayList<String> IPList, HashMap<String, Integer> PortList, HashMap<String, Integer> IPSPortList, int IPS_PORT, Socket socket){

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

    private void IPServerLBProc(){

        System.out.println("LB connected");

        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String remoteHostName = remoteSocketAddress.getAddress().getHostAddress();
        int LBIdentifier = 0;

        try {
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            String connType = dataInputStream.readUTF();

            if(connType.equals("conn")){

                synchronized (IPList){

                    LBIdentifier = IPList.size();
                    IPList.add(remoteHostName);
                }

                try {
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(PortList);
                    objectOutputStream.flush();
//            objectOutputStream.close();

                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeInt(LBIdentifier);
                    dataOutputStream.flush();

                    if(LBIdentifier == 0)
                        dataOutputStream.writeUTF(remoteHostName);
                    else
                        dataOutputStream.writeUTF(IPList.get(0));

                    dataOutputStream.flush();
                    dataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            else if(connType.equals("elect")){

                int curMaster = dataInputStream.readInt();
                String LBMaster;
                synchronized (IPList){

                    LBMaster = IPList.get(curMaster);
                }

                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(LBMaster);
                dataOutputStream.flush();
                dataOutputStream.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void IPServerBrokerProc(){

        System.out.println("Broker connected");

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

    private void IPServerClientProc(){

        System.out.println("Client connected");

        String temp;
        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String remoteHostName = remoteSocketAddress.getAddress().getHostAddress();

        synchronized (IPList){
            temp = IPList.get(Math.abs(MurmurHash.hash32(remoteHostName)) % IPList.size());
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
