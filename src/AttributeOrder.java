public class AttributeOrder {

    String[] attributes;
    int[] order = new int[GlobalState.NumberOfDimensions];

    public AttributeOrder(){

        this.attributes = GlobalState.attributes;

        for (int i = 0; i < GlobalState.NumberOfDimensions; i++) {

            order[i] = i;
        }
    }
}
