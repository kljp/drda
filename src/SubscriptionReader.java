import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class SubscriptionReader {

    public static void main(String[] args) {

        generatePlot(readSubscriptions());
    }

    public static ArrayList<msgEPartition> readSubscriptions(){

        msgEPartition message;
        String output = GlobalState.subscriptionListDir;
        ArrayList<msgEPartition> list = new ArrayList<>();

        try {
            FileInputStream inputStream = new FileInputStream(output);
            while (true) {

                message = msgEPartition.parseDelimitedFrom(inputStream);
                if (message == null)
                    break;
                list.add(message);
//                System.out.println(message.toString());
            }

            inputStream.close();

//            for (msgEPartition msg : list)
//                System.out.println(msg.toString());
//
//            System.out.println(list.size());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

    public static void generatePlot(ArrayList<msgEPartition> list) {

        FileOutputStream outputStream;
        double avg;
        String output;
        try {
            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                output = "./src/plot/plot" + i;
                outputStream = new FileOutputStream(output);

                for (msgEPartition msg : list) {

                    avg = Math.round(((msg.getSub().getLowerBound(i) + msg.getSub().getUpperBound(i)) / 2.0) * 100) / 100.0;
                    outputStream.write(Double.toString(avg).getBytes());
                    outputStream.write("\n".getBytes());
                }

                outputStream.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}