package receipts.Util;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import receipts.result_objects.CategoryDiscountRecord;
import receipts.result_objects.StateTotalRecord;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static receipts.conf.DataSource.OUTPUT_DIR;

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


    /*
        get the Date object of m month before the current date
     */

    public static Date getDateByMonthBefore(Integer m) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.MONTH, -m);
        return calendar.getTime();
    }


    /*
        JavaPairRDD to JSON file
     */

    public static void datasetToJSON(Dataset<Row> input, String fileName) {

        input.write().mode(SaveMode.Overwrite).json(OUTPUT_DIR + fileName);
        try {
            toOneFile(input, fileName, "json");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /*
        JavaPairRDD to CSV
     */

    public static void datasetToCSV(Dataset<Row> input, String fileName) {

        input.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").csv(OUTPUT_DIR + fileName);
        try {
            toOneFile(input, fileName, "csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /*
        Dataset to parquet
     */

    public static void datasetToParquet(Dataset<Row> input, String fileName) {
        input.write().mode(SaveMode.Overwrite).parquet(OUTPUT_DIR + fileName);
        try {
            toOneFile(input, fileName, "parquet");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /*
        put data from .crc files to one file
     */

    private static void toOneFile(Dataset<Row> input, String fileName, String fileType) throws IOException {

        Configuration hadoopConfig = new Configuration();
        FileSystem hdfs = FileSystem.get(hadoopConfig);

        Path srcPath=new Path(OUTPUT_DIR + fileName);
        Path destPath= new Path(OUTPUT_DIR + fileName + "_merged2." + fileType);
        File srcFile= Arrays
                .stream(FileUtil.listFiles(new File(OUTPUT_DIR + fileName)))
                .filter(f -> f.getPath().endsWith("." + fileType))
                .findFirst().orElse(null);

        //Copy the CSV file outside of Directory and rename
        assert srcFile != null;
        FileUtil.copy(srcFile,hdfs,destPath,true,hadoopConfig);

        //Remove Directory created by df.write()
        hdfs.delete(srcPath,true);

        //Removes CRC File
        hdfs.delete(new Path(OUTPUT_DIR + "." + fileName + "_merged2." + fileType + ".crc"),true);

        // Merge Using Haddop API
        input.repartition(1).write().mode(SaveMode.Overwrite)
                .csv(OUTPUT_DIR + fileName + "-tmp");
        Path srcFilePath=new Path(OUTPUT_DIR + fileName + "-tmp");
        Path destFilePath= new Path(OUTPUT_DIR + fileName + "_merged." + fileType);
        FileUtil.copyMerge(hdfs, srcFilePath, hdfs, destFilePath, true, hadoopConfig, null);

        //Remove hidden CRC file if not needed.
        hdfs.delete(new Path(OUTPUT_DIR + "." + fileName + "_merged." + fileType + ".crc"),true);
        hdfs.delete(new Path(OUTPUT_DIR + fileName + "_merged2." + fileType),true);
    }

}
