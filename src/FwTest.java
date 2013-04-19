
import java.math.BigInteger;



/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author marc
 */
public class FwTest {
    
    public static void main(String[] args) {
        BigInteger t1=BigInteger.valueOf(3L);
        BigInteger t2=BigInteger.valueOf(1L);
        BigInteger t3=t1.shiftLeft(64).add(t2);
        System.out.println("->"+t3.toString(2));
    }
    
}
