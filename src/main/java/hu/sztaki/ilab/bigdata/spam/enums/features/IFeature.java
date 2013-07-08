package hu.sztaki.ilab.bigdata.spam.enums.features;

/**
 *
 * @author garzo
 */
public interface IFeature {
    
    public int index();
    public int index_maxmp();
    public int index_hp();
    public String featureName();
    
}
