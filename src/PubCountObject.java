public class PubCountObject {

    private double[] lowerbounds = new double[GlobalState.NumberOfDimensions];
    private double[] upperbounds = new double[GlobalState.NumberOfDimensions];
    private int pubCount;

    public PubCountObject(double[] lowerbounds, double[] upperbounds){

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

            this.lowerbounds[i] = lowerbounds[i];
            this.upperbounds[i] = upperbounds[i];
            this.pubCount = 0;
        }
    }

    public void setPubCount(int pubCount){

        this.pubCount = pubCount;
    }

    public int getPubCount(){

        return this.pubCount;
    }

    public double getNthLowerBound(int n){

        return this.lowerbounds[n];
    }

    public double getNthUpperBound(int n){

        return this.upperbounds[n];
    }
}
