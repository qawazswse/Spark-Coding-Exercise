package receipts.Util;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;

import java.io.*;
import java.util.*;

import static receipts.conf.DataSourceConf.OUTPUT_DIR;

public class Util {

    // turn a Double value to two-digits String
    public static String toTwoDigitsDouble(Object o) {
        return String.format("%.2f", o);
    }


    /**
        the original csv file has cells that contain comma, which can be a problem using sc.textFile() with split(",")
        make a modified version of the cvs file to solve this problem
        @param fileDir: Original file direction as String
        @return : modified version file direction as String
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


    /**
        get the Date object of m month before the current date
        @param months : analyse data in how many months
        @return : the date before input months
     */

    public static Date getDateByMonthBefore(Integer months) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.MONTH, -months);
        return calendar.getTime();
    }


    /**
        put data from .crc files to one file
        @param input : the input data as JavaRDD
        @param fileName : the name of the output file
        @param fileType : the type choice of the output file
     */

    public static void toOneFile(JavaRDD<? extends Serializable> input, String fileName, String fileType) throws IOException {

        Configuration hadoopConfig = new Configuration();
        FileSystem hdfs = FileSystem.get(hadoopConfig);

        Path srcPath=new Path(OUTPUT_DIR + fileName);
        Path destPath= new Path(OUTPUT_DIR + fileName + "_merged2." + fileType);
        File srcFile= Arrays
                .stream(FileUtil.listFiles(new File(OUTPUT_DIR + fileName)))
                .filter(f -> f.getPath().contains("part"))
                .findFirst().orElse(null);

        //Copy the CSV file outside of Directory and rename
        assert srcFile != null;
        FileUtil.copy(srcFile,hdfs,destPath,true,hadoopConfig);

        //Remove Directory created by df.write()
        hdfs.delete(srcPath,true);

        //Removes CRC File
        hdfs.delete(new Path(OUTPUT_DIR + "." + fileName + "_merged2." + fileType + ".crc"),true);

        // Merge Using Haddop API
        input.coalesce(1, true).saveAsTextFile(OUTPUT_DIR + fileName + "-tmp");
        Path srcFilePath=new Path(OUTPUT_DIR + fileName + "-tmp");
        Path destFilePath= new Path(OUTPUT_DIR + fileName + "_merged." + fileType);
        File file = new File(OUTPUT_DIR + fileName + "_merged." + fileType);
        if(file.exists()) {
            hdfs.delete(new Path(OUTPUT_DIR + fileName + "_merged." + fileType),true);
        }
        FileUtil.copyMerge(hdfs, srcFilePath, hdfs, destFilePath, true, hadoopConfig, null);
        //Remove hidden CRC file if not needed.
        hdfs.delete(new Path(OUTPUT_DIR + "." + fileName + "_merged." + fileType + ".crc"),true);
        hdfs.delete(new Path(OUTPUT_DIR + fileName + "_merged2." + fileType),true);
    }

}
