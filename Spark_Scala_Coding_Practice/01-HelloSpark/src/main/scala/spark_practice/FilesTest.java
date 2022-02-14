package spark_practice;

import com.google.common.io.LineReader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * created by ANEK on Monday 4/5/2021 at 11:38 PM
 */

public class FilesTest {
    public static void main(String[] args) throws IOException {

        List<List<Integer>> arr = new ArrayList<>();

        Integer[][] a = new Integer[arr.size()][];
        for (int i=0; i<arr.size(); i++) {
            List<Integer> row = arr.get(i);
            // row.toArray(new Integer[row.size()])   ;
            a[i] = row.toArray(new Integer[row.size()]);
        }
        System.out.println(Arrays.deepToString(a));



        Path path = Paths.get("F:\\Spark_workspace\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\main" +
                "\\resources\\db_config.properties");

        /*   Method 1       */

        Path path2 = Paths.get("./src/main/resources/db_config.properties");

        InputStream inputStreamFile = new FileInputStream(path2.toFile()) ;
        Properties properties1 = new Properties();
        properties1.load(inputStreamFile);
        System.out.println("properties1 Paths.get:: "+properties1.getProperty("db.url"));


        List<String> lines = Files.readAllLines(path2);
        //System.out.println(lines);


        /*   Method 2      */

        //  always loads the file from resources folder
        InputStream isLoader = DateTest.class.getClassLoader().getResourceAsStream("db_config.properties");
        InputStreamReader isReader = new InputStreamReader(isLoader);
        BufferedReader bufferedReader = new BufferedReader(isReader);
        String line;
        while( ( line = bufferedReader.readLine() )!= null) {
            // Process line
            System.out.println("line getClassLoader :: -  "+line);
        }


        // here inputStream object " is "  is of type BufferedInputStream which is not compatible with properties.load
        Properties properties2 = new Properties();
        System.out.println("is  :: "+isLoader);
        properties2.load(isLoader);
        System.out.println("properties2 geClassLoader:: "+properties2.getProperty("db.url"));

        InputStream inputStream = new BufferedInputStream(new FileInputStream("F:\\Spark_workspace" +
                "\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\main\\resources\\db_config.properties"));
        Properties properties3 = new Properties();
        properties3.load(inputStream);

        System.out.println("properties  BufferedInputStream:: "+properties3.getProperty("db.url"));





        File jarPath = new File(DateTest.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        System.out.println(" jarPath :: " +jarPath );
        String absolutePath = jarPath.getParentFile().getCanonicalPath();

        System.out.println("absolutePath :: "+absolutePath);

        File p = new File("./../");
        System.out.println(p);



    }
}
