import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class PublicationReader {

    public static void main(String[] args) {

        generatePlot(readPublications());
    }

    public static ArrayList<msgEPartition> readPublications(){

        msgEPartition message;
        String output = GlobalState.publicationListDir;
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
        double point;
        String output;
        try {
            for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

                output = "./src/plot2/plot" + i;
                outputStream = new FileOutputStream(output);

                for (msgEPartition msg : list) {

                    point = msg.getPub().getSinglePoint(i);
                    outputStream.write(Double.toString(point).getBytes());
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