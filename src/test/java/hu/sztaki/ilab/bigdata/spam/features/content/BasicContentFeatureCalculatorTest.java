package hu.sztaki.ilab.bigdata.spam.features.content;

import hu.sztaki.ilab.bigdata.spam.MockParserResult;
import hu.sztaki.ilab.bigdata.spam.enums.features.BasicContentFeatures;

import java.util.Map;

import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class BasicContentFeatureCalculatorTest extends TestCase {
    
    private final int testPageNum = 3;
    private MockParserResult[] parserResults = new MockParserResult[testPageNum];
    private Map[] features = new Map[testPageNum];
    private final BasicContentFeatureCalculator calculator = new BasicContentFeatureCalculator();

    public BasicContentFeatureCalculatorTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        MockParserResult page1 = new MockParserResult.MockParserBuilder("http://host1.com")
                .title("short title here")
                .content("somewhere somehow something someone")
                .withOutlink("http://", "achor text")
                .build();
        parserResults[0] = page1;

        MockParserResult page2 = new MockParserResult.MockParserBuilder("http://host2.com")
                .title("shorter title")
                .content("a b c d e")
                .withOutlink("http://", "achor text")
                .build();
        parserResults[1] = page2;

        MockParserResult page3 = new MockParserResult.MockParserBuilder("http://host3.com")
                .title("")
                .content("")
                .build();
        parserResults[2] = page3;

        for (int i = 0; i < parserResults.length; i++) {
            calculator.processContent(parserResults[i].getParseResult(),
                    parserResults[i].getMetaData());
            features[i] = calculator.getFeatures();
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testAvgLengthAndWordCount() {
        // word count
        assertEquals(4.0, features[0].get(BasicContentFeatures.WordCount.index()));
        assertEquals(5.0, features[1].get(BasicContentFeatures.WordCount.index()));
        assertEquals(0.0, features[2].get(BasicContentFeatures.WordCount.index()));

        // avg word length
        assertEquals(8.0, features[0].get(BasicContentFeatures.AvgLength.index()));
        assertEquals(1.0, features[1].get(BasicContentFeatures.AvgLength.index()));
        assertEquals(0.0, features[2].get(BasicContentFeatures.AvgLength.index()));

        // number of title words
        assertEquals(3.0, features[0].get(BasicContentFeatures.NumTitleWords.index()));
        assertEquals(2.0, features[1].get(BasicContentFeatures.NumTitleWords.index()));
        assertEquals(0.0, features[2].get(BasicContentFeatures.NumTitleWords.index()));
    }

    public void testFractionOfVisibleContent() {
        // fraction of visible content
        assertEquals((32.0 / 35.0), features[0].get(BasicContentFeatures.FracVisible.index()));
        assertEquals((5.0 / 9.0), features[1].get(BasicContentFeatures.FracVisible.index()));
        assertEquals(0.0, features[2].get(BasicContentFeatures.FracVisible.index()));

        // fraction of anchor text
        assertEquals(0.5, features[0].get(BasicContentFeatures.FracAnchor.index()));
        assertEquals(0.4, features[1].get(BasicContentFeatures.FracAnchor.index()));
        assertEquals(0.0, features[2].get(BasicContentFeatures.FracAnchor.index()));
    }

    public void testCompressionRate() {
        assertEquals(1.0, features[0].get(BasicContentFeatures.CompressRate.index()));
        assertEquals(0.0, features[1].get(BasicContentFeatures.CompressRate.index()));
        assertEquals(0.0, features[2].get(BasicContentFeatures.CompressRate.index()));
    }

    public void testEntropyAndLikelihood() {
        // entropy
        assertEquals(1.38629436, (Double)(features[0].get(BasicContentFeatures.Entropy.index())), 1e+4);
        assertEquals(1.60943791, (Double)(features[1].get(BasicContentFeatures.Entropy.index())), 1e+4);
        assertEquals(0.0, features[2].get(BasicContentFeatures.Entropy.index()));

       // independent likelihood
        assertEquals(0.69314718, (Double)(features[0].get(BasicContentFeatures.IndepLikelihood.index())), 1e+4);
        assertEquals(1.0986123, (Double)(features[1].get(BasicContentFeatures.IndepLikelihood.index())), 1e+4);
        assertEquals(0.0, features[2].get(BasicContentFeatures.IndepLikelihood.index()));               
    }

}
