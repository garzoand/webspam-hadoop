package hu.sztaki.ilab.bigdata.spam.enums;

public enum ItemSets {

    K_FREQUENCY_8(8),
    K_FREQUENCY_100(100),
    K_FREQUENCY_200(200),
    K_FREQUENCY_500(500),
    K_FREQUENCY_1000(1000);
    
    private int size;
    
    private ItemSets(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }
    
}
