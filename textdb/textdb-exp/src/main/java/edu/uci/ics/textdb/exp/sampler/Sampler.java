package edu.uci.ics.textdb.exp.sampler;


import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;

import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.sampler.SamplerPredicate.SampleType;

/**
 * @author Qinhua Huang
 * 
 * This class is to sample k tuples from the tuples stream using reservoir sampling algorithm.
 * The probability of sampling for each tuple is k/sizeof(all tuples).
 * 
 * Example:
 * Tuples: 1,2,3,4,5,6,7,8,9,10
 * sample size k: 3
 * output:
 * the result is in randomly uniform distribution.
 * one possible result:
 *  1, 6, 2
 *        
 * 
 */

public class Sampler extends AbstractSingleInputOperator implements ISourceOperator{

    private SamplerPredicate predicate;
    private List<Tuple> sampleBuffer;
    private int bufferCursor;
    private Schema inputSchema;
    
    public Sampler(SamplerPredicate predicate) {
        this.predicate = predicate;
        this.sampleBuffer = null;
        this.bufferCursor = -1;
        
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        
    }

    public void constructSampleBuffer() throws TextDBException {
        sampleBuffer = new ArrayList<Tuple>();
        
        Random genRandom;
        genRandom = new Random(System.currentTimeMillis());
        
        Tuple tuple;
        int count = 0;
        while ((tuple = inputOperator.getNextTuple()) != null) {
            if (count < predicate.getSampleSize()) {
                sampleBuffer.add(tuple);
            } else {
                // Exit the loop in topK mode.
                if (this.predicate.getSampleType() == SampleType.FIRST_K_ARRIVAL) {
                    break;
                }
                /*
                 *  Using reservoir sampling method to sample tuples.
                 *  In effect, for all tuples, the ith tuple is chosen 
                 *  to be included in the reservoir with probability
                 *  sampleSize / i.
                 */
                if (this.predicate.getSampleType() == SampleType.RANDOM_SAMPLE) {
                    int randomPos = genRandom.nextInt(count);
                    if (randomPos < predicate.getSampleSize()) {
                        sampleBuffer.set(randomPos, tuple);
                    }
                }
            }
            count++;
        }
    }
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        if (predicate.getSampleSize() < 1) {
            return null;
        }
        if (sampleBuffer == null) {
            constructSampleBuffer();
            this.bufferCursor = 0;
        }
        if (sampleBuffer == null || bufferCursor == sampleBuffer.size()) {
            return null;
        }

        // get an output tuple.
        Tuple resultTuple = sampleBuffer.get(bufferCursor);
        bufferCursor++;

        return resultTuple;
    }
    
    @Override
    protected void cleanUp() throws TextDBException {
        sampleBuffer = null;
        bufferCursor = -1;
    }
    
    public SamplerPredicate getPredicate() {
        return this.predicate;
    }
    
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        throw new RuntimeException("Sampler does not support process one tuple");
    }
}

