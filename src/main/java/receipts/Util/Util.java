package receipts.Util;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Util {

    // turn a Double value to two-digits String
    public static String toTwoDigitsDouble(Object o) {
        return String.format("%.2f", (Double)o);
    }

    /*
        the original csv file has cells that contain comma, which can be a problem using sc.textFile() with split(",")
        make a modified version of the cvs file to solve this problem
        input: Original file direction as String
        output: modified version file direction as String
     */
    public static String CommaToQuestionMarkInCSV(String fileDir) throws IOException {

        String newDir = fileDir.substring(0, fileDir.length()-4) + "_modified.csv";
        File inputFile = new File(fileDir);
        File outputFile = new File(newDir);

        // Read existing file
        CSVReader reader = new CSVReader(new FileReader(inputFile), ',');
        List<String[]> csvBody = reader.readAll();

        // get CSV row column  and replace with by using row and column
        int cnt=0;
        for(String[] row : csvBody) {
            for(int i=0; i<row.length; i++) {
                if(row[i].contains(",")) {
                    row[i] = row[i].replace(",", "?");
                }
            }
//            System.out.println(row.length + " " + ++cnt + "" + Arrays.toString(row));
        }
        reader.close();

        // Write to CSV file which is open
        CSVWriter writer = new CSVWriter(new FileWriter(outputFile), ',', CSVWriter.NO_QUOTE_CHARACTER);
        writer.writeAll(csvBody);
        writer.flush();
        writer.close();

        //return the new file dir as String
        return newDir;
    }

}
