package spark_practice.Java_Generics;

import java.util.Comparator;

public class GenericsTest {
    public static void main(String[] args) {
        String s1 = "hello";
        String s2 = "gello";
        System.out.println(s1.equals(s2));
        System.out.println(s1.compareTo(s2));

        Emp22 e1 = new Emp22(10);
        Emp22 e2 = new Emp22(11);

        System.out.println(e1);
        System.out.println(e2);

        System.out.println(e1.compare(e1,e2));

    }
}

class Emp22 implements Comparator {
    int a ;
    Emp22(int a ){
        this.a = a;
    }
     public int getA(){return a ;}

     public String toString(){
        return "a -> "+this.a;
     }
    @Override
    public int compare(Object o1, Object o2) {
        Emp22 o11=null;Emp22 o22=null;
        if(o1 instanceof Emp22 && o2 instanceof Emp22){
            o11 = (Emp22)o1;
            o22 = (Emp22)o2;
        }
        return o11.getA() - o22.getA();
    }

}