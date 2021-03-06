import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;

public class IPServerMain {

    private static ArrayList<String> IPList = new ArrayList<String>(); // The list of load balancers
    private static HashMap<String, Integer> PortList = new HashMap<String, Integer>();
    private static HashMap<String, Integer> IPSPortList = new HashMap<String, Integer>();

    public static void main(String[] args) {

        PortList.put("LB_LB_PORT", 5001); // Convention: (Server_Client_Port, PORT)
        PortList.put("LB_SUB_PORT", 5002);
        PortList.put("LB_PUB_PORT", 5003);
        PortList.put("LB_BROKER_PUB_PORT", 5004);
        PortList.put("LB_BROKER_SUB_PORT", 5005);
        PortList.put("SUB_BROKER_PORT", 5006);
        PortList.put("BROKER_LB_SYNC_PORT", 5010);

        IPSPortList.put("IPS_LB_PORT", 5007); // Convention: (Server_Client_Port, PORT)
        IPSPortList.put("IPS_BROKER_PORT", 5008);
        IPSPortList.put("IPS_CLIENT_PORT", 5009);

        try {
            new IPServerConnThread(IPList, PortList, IPSPortList, IPSPortList.get("IPS_LB_PORT"), new ServerSocket(IPSPortList.get("IPS_LB_PORT"))).start();
            new IPServerConnThread(IPList, PortList, IPSPortList, IPSPortList.get("IPS_BROKER_PORT"), new ServerSocket(IPSPortList.get("IPS_BROKER_PORT"))).start();
            new IPServerConnThread(IPList, PortList, IPSPortList, IPSPortList.get("IPS_CLIENT_PORT"), new ServerSocket(IPSPortList.get("IPS_CLIENT_PORT"))).start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
