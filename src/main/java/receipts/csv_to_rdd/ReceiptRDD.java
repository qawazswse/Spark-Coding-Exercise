package receipts.csv_to_rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import receipts.Util.Util;
import receipts.case_objects.Receipt;

import java.io.IOException;
import java.text.SimpleDateFormat;

import static receipts.conf.DataSource.RECEIPTS_COLUMN_NUMBER;

/*
    methods that read data from csv files to a JavaRDD of corresponding case object
 */
public class ReceiptRDD {


    /*
        input: JavaSparkContext, and file direction as a String
        output: raw data JavaRDD
     */

    public static JavaRDD<Receipt> setupReceiptsDataRdd(JavaSparkContext sc, String fileDir) {

        // using the modified file instead of the original one
        try {
            fileDir = Util.CommaToQuestionMarkInCSV(fileDir);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Cannot find file: " + fileDir);
        }

        SimpleDateFormat formatter = new SimpleDateFormat("dd-M-yyyy hh:mm:ss.sss");

        return sc.textFile(fileDir)
                .mapPartitionsWithIndex((index, iter) -> {  // remove the header row
                    if (index == 0 && iter.hasNext()) {
                        iter.next();
                    }
                    return iter;
                }, true)
                .map(row -> {     // make the value(state and receipt total) into Receipt Objects

                    String[] rawCells = row.split(",");

                    // for the cases that the last few cell is empty
                    // make sure the cells String array is long enough
                    String[] cells = new String[RECEIPTS_COLUMN_NUMBER];
                    System.arraycopy(rawCells, 0, cells, 0, rawCells.length);
                    for(int i=0; i<cells.length; i++) {
                        if(cells[i] == null) {
                            cells[i] = "";
                        }
                    }

                    return new Receipt(
                            cells[0].length()>0? cells[0] : null,
                            cells[1].length()>0? cells[1] : null,
                            cells[2].length()>0? cells[2] : null,
                            cells[3].length()>0? cells[3] : null,
                            cells[4].length()>0? cells[4] : null,
                            cells[5].length()>0? cells[5] : null,
                            cells[6].length()>0? cells[6] : null,
                            cells[7].length()>0? Integer.parseInt(cells[7]) : null,
                            cells[8].length()>0? cells[8] : null,
                            cells[9].length()>0? formatter.parse(cells[9]) : null,
                            cells[10].length()>0? formatter.parse(cells[10]) : null,
                            cells[11].length()>0? Double.parseDouble(cells[11]) : null,
                            cells[12].length()>0? Integer.parseInt(cells[12]) : null,
                            cells[13].length()>0? cells[13] : null,
                            cells[14].length()>0? formatter.parse(cells[14]) : null,
                            cells[15].length()>0? Boolean.parseBoolean(cells[15]) : null
                    );
                });
    }
}
