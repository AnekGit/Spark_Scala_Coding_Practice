package java_practice;

import java.util.Scanner;

/**
 * created by ANEK on Sunday 1/23/2022 at 8:17 PM
 */

public class Recursion_Ex {
    public static void main(String[] args) {

    // calculate power
    /*
    *    pow(2,5) =  2 * 2 * 2 * 2 * 2
    *    pow(2,5) =  (2 * 2 * 2 * 2) 4 times  * 2
    *    pow(2,n) =  pow(2,n-1) * m
    **************************************************
    *    pow(m,n) =  pow(m,n-1) * m
    *
    * Now ,
    *
    *       pow(m,n) = 1                      if n = 0
    *                  pow(m,n-1) * m         if n > 0
    *
    **************************************************
    * */
        Scanner sc = new Scanner(System.in);

        System.out.println("enter m and n :: ");
        int m = Integer.parseInt(sc.nextLine());
        int n = Integer.parseInt(sc.nextLine());

        int power = pow(m,n);
        System.out.println("power of "+m+" , n "+n+ " :: "+power);
    }
    public static int pow(int m ,int n ){
        if(n == 0 ) return 1;
        return pow(m,n-1) * m ;

    }

}

