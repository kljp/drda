import com.EPartition.EPartitionMessageSchema.msgEPartition;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class SubscriptionWriter {

    public static void main(String[] args) {

        String msgType = "Subscription";
        MessageWrapper messageWrapper;
        msgEPartition message;
        String output = GlobalState.subscriptionListDir;
        FileOutputStream outputStream;

        try {
            outputStream = new FileOutputStream(output);

            for (int i = 0; i < GlobalState.NumberOfSubscriptions; i++) {

                messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomRangeGenerator());
                messageWrapper.buildMsgEPartition().writeDelimitedTo(outputStream);
            }

            outputStream.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}