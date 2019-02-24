package learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author behzad.pirvali, 8/21/18
 */

public class DriverAnimalJava {
    static class Animal implements Comparable<Animal>  {

        public int compareTo(Animal o) {
            return 0;
        }
    }
    static class Lion extends Animal {}
    static class Zebra extends Animal {}

    public static <A extends Comparable<? super A>> void sort(List<A> list) {
        Collections.sort(list);
    }

    public static void main(String... args) {
        List<Animal> lions = new ArrayList<>();
        lions.add(new Lion());
        lions.add(new Lion());
        sort(lions);

        List<Animal> zoo = new ArrayList<>();
        zoo.add(new Lion());
        zoo.add(new Lion());
        zoo.add(new Zebra());
        sort(zoo);

    }
}
