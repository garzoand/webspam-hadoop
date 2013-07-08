package hu.sztaki.ilab.bigdata.spam.enums.features;

/**
 * Marker interface for Corpus/Query precision and recall features
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 */
public interface IPrecisionRecallFeature extends IFeature {

    public int getPrecisionBaseIndex();
    public int getRecallBaseIndex();
    
}
