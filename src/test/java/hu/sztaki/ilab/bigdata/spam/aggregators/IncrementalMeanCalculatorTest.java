/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.spam.aggregators;

import hu.sztaki.ilab.bigdata.common.utils.IncrementalMeanCalculator;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class IncrementalMeanCalculatorTest extends TestCase {
    
    private IncrementalMeanCalculator calculator = new IncrementalMeanCalculator();
    private double[] numbers = { 1.0, 2.0, 3.0, 4.0, 5.0 };
    private double[] means = { 1.0, 1.5, 2.0, 2.5, 3.0 };
    private double[] variances = { 0.0, 0.5, 1.0, 1.6666666667, 2.5 };

    public IncrementalMeanCalculatorTest(String testName) {
        super(testName);
    }

    @Override
    public void setUp() {
        
    }

    public void testGetMean() {
        calculator.reset();
        for (int i = 0; i < numbers.length; i++) {
            calculator.add(numbers[i]);
            assertEquals(means[i], calculator.getMean(), 1e-5);
            assertEquals(variances[i], calculator.getVariance(), 1e-5);
        }
    }

}
