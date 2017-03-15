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
                System.out.println("\n random = "+randomPos);
                if (randomPos < revSize) {
                    list.set(randomPos, j);
                }
            }
        }
        System.out.println(list);
        for (int i = 0; i<10; i++){
            System.out.println(genRandom.nextInt(10));
        }
    }

}
