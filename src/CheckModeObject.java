import java.io.Serializable;

public class CheckModeObject implements Serializable {

    int mode;

    public CheckModeObject(int mode){

        this.mode = mode;
    }

    public void setMode(int mode){

        this.mode = mode;
    }

    public int getMode(){

        return this.mode;
    }
}
