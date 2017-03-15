package edu.uci.ics.textdb.dataflow.sampler;


import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;

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

    private List<Tuple> reservoirTupleBuffer;
    private int bufferCursor;
;
    private Schema inputSchema;
    
    public Sampler(SamplerPredicate predicate) {
        this.predicate = predicate;
        reservoirTupleBuffer = null;
        this.bufferCursor = -1;

    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
    }

    public void sampleTuples() throws TextDBException {
        reservoirTupleBuffer = new ArrayList<Tuple>();
        
        Random genRandom;
        genRandom = new Random(System.currentTimeMillis());
        
        Tuple tuple;
        int count = 0;
        while ((tuple = inputOperator.getNextTuple()) != null) {
            if (count < predicate.getReservoirSize()) {
                reservoirTupleBuffer.add(tuple);
            } else {
                // In effect, for all tuples, the ith tuple is chosen to be included in the reservoir with probability
                // ReservoirSize / i.
                int randomPos = genRandom.nextInt(count);
                if (randomPos < predicate.getReservoirSize()) {
                    reservoirTupleBuffer.set(randomPos, tuple);
                }
            }
            count++;
        }
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        if (predicate.getReservoirSize() < 1) {
            return null;
        }
        if (reservoirTupleBuffer == null || reservoirTupleBuffer.size() == 0) {
            sampleTuples();
            if (this.reservoirTupleBuffer.size() > 0) {
                this.bufferCursor = 0;
            }
        }
        if (bufferCursor == reservoirTupleBuffer.size())
            return null;

        //If there is a buffer and cursor < reservoirTupleBuffer.size, get an output tuple.
        Tuple resultTuple = reservoirTupleBuffer.get(bufferCursor);
        bufferCursor++;
        // If it reaches the end of the buffer, reset the buffer cursor.
        if (bufferCursor == reservoirTupleBuffer.size()) {
            reservoirTupleBuffer = null;
            bufferCursor = 0;
        }
        
        return resultTuple;
    }
    
    
    @Override
    protected void cleanUp() throws TextDBException {
        reservoirTupleBuffer = null;
        bufferCursor = 0;
    }
    
    public SamplerPredicate getPredicate() {
        return this.predicate;
    }

    
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        throw new RuntimeException("RegexSplit does not support process one tuple");
    }
}

