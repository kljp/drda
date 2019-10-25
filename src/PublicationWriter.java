import com.EPartition.EPartitionMessageSchema;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class PublicationWriter {

    public static void main(String[] args) {

        String msgType = "Publication";
        MessageWrapper messageWrapper;
        EPartitionMessageSchema.msgEPartition message;
        String output = GlobalState.publicationListDir;
        FileOutputStream outputStream;

        try {
            outputStream = new FileOutputStream(output);

            for (int i = 0; i < GlobalState.NumberOfPublications; i++) {

                messageWrapper = new MessageWrapper(msgType, new RangeGenerator().randomValueGenerator());
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
