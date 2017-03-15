package edu.uci.ics.textdb.sandbox.helloworldexamples;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.uci.ics.textdb.api.tuple.Tuple;

public class HelloRandom {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        sampleReservoir(5);
    }
    
    public static void sampleReservoir(int k) {
        List<Integer> list = null;
        list = new ArrayList<Integer>();
        Random genRandom;
        genRandom = new Random(System.currentTimeMillis());
        int revSize = k;
        
        int count = 0;
        for (int j =0; j<10; j++) {
            if (j < revSize) {
                list.add(j);
            } else {
                // In effect, for all tuples, the ith tuple is chosen to be included in the reservoir with probability
                // ReservoirSize / i.
                int randomPos = genRandom.nextInt(j);
                if (randomPos < revSize) {
                    list.set(randomPos, j);
                }
            }
        }
        System.out.println(list);
        List<Integer> counter = null;
        counter = new ArrayList<Integer>();
        for (int i = 0; i<10; i++) {
            counter.add(i,0);
        }
        /*
         * Randomly select a number between [1:n].
         * See what is the distribution about.
         */
        for (int i = 0; i<10000; i++) {
            int selector = genRandom.nextInt(4);
            counter.set(selector, counter.get(selector)+1);
        }
        System.out.println(counter);
    }

}
