package spark_practice;

import javax.validation.constraints.NotNull;

/**
 * created by ANEK on Tuesday 3/9/2021 at 11:27 AM
 */

public class Emp {

       @NotNull
        int id;
        String name ;
        String dept;

        public Emp(int id, String name, String dept) {
            this.id = id;
            this.name = name;
            this.dept = dept;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDept() {
            return dept;
        }

        public void setDept(String dept) {
            this.dept = dept;
        }
    }

