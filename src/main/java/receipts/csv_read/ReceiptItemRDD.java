package receipts.csv_read;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import receipts.Util.Util;
import receipts.case_objects.ReceiptItem;

import java.io.IOException;

import static receipts.conf.DataSourceConf.RECEIPTS_ITEM_COLUMN_NUMBER;

public class ReceiptItemRDD {

    /**
     * read receipts items CSV file into javaRDD object
        @param sc : JavaSparkContext
        @param fileDir : file direction as a String
        @return : raw data JavaRDD
     */

    public static JavaRDD<ReceiptItem> setupReceiptsItemDataRdd(JavaSparkContext sc, String fileDir) {

        // using the modified file instead of the original one
        try {
            fileDir = Util.CommaToQuestionMarkInCSV(fileDir);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Cannot find file: " + fileDir);
        }

        return sc.textFile(fileDir)
                .mapPartitionsWithIndex((index, iter) -> {  // remove the header row
                    if (index == 0 && iter.hasNext()) {
                        iter.next();
                    }
                    return iter;
                }, true)
                .map(row -> {     // make the value(state and receipt total) into ReceiptItem Objects

                    String[] rawCells = row.split(",");

                    // for the cases that the last few cell is empty
                    // make sure the cells String array is long enough
                    String[] cells = new String[RECEIPTS_ITEM_COLUMN_NUMBER];
                    System.arraycopy(rawCells, 0, cells, 0, rawCells.length);
                    for(int i=0; i<cells.length; i++) {
                        if(cells[i] == null) {
                            cells[i] = "";
                        }
                    }

                    return new ReceiptItem(
                            cells[0].length()>0? cells[0] : null,
                            cells[1].length()>0? Integer.parseInt(cells[1]) : null,
                            cells[2].length()>0? cells[2] : null,
                            cells[3].length()>0? cells[3] : null,
                            cells[4].length()>0? cells[4] : null,
                            cells[5].length()>0? Integer.parseInt(cells[5]) : null,
                            cells[6].length()>0? Double.parseDouble(cells[6]) : null,
                            cells[7].length()>0? Double.parseDouble(cells[7]) : null,
                            cells[8].length()>0? Double.parseDouble(cells[8]) : null,
                            cells[9].length()>0? cells[9] : null,
                            cells[10].length()>0? cells[10] : null,
                            cells[11].length()>0? cells[11] : null,
                            cells[12].length()>0? cells[12] : null
                    );
                });
    }
}
