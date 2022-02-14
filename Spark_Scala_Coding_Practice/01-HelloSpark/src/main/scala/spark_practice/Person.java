package spark_practice;

import java.io.Serializable;
import java.util.Objects;

/**
 * created by ANEK on Friday 3/12/2021 at 12:27 PM
 */

public class Person  implements Serializable {

        Integer id ;
        public Person() {

        }
        public Person(Integer id) {
            this.id = id;
        }

        public Integer getId() {
            return id;
        }

        public spark_practice.Person setId(Integer id) {
            this.id = id;return this;
        }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(getId(), person.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }

    @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    '}';
        }
    }

