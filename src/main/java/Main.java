import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import receipts.analyze.*;

public class Main {

    public static void main(String[] args) {

        // check parameters number
        if(args.length != 3) {
            System.out.println("parameters number invalid " +
                    "(there should be 3 parameters but " +
                    args.length
            );
            return;
        }

        // make sure the first parameter is valid
        if(args[0].length()>1
                || !Character.isDigit(args[0].charAt(0))
                ||  Integer.parseInt(args[0]) < 0
                ||  Integer.parseInt(args[0]) < 0) {
            System.out.println("The 1ST parameter is invalid");
            return;
        }

        // make sure the second parameter is valid
        for(int i=0; i<args[1].length(); i++) {
            if(!Character.isDigit(args[1].charAt(i))) {
                System.out.println("The 2ND parameter is invalid");
                return;
            }
        }

        // make sure the third parameter is valid
        if(!args[2].equals("csv") && !args[2].equals("json") && !args[2].equals("parquet")) {
            System.out.println("The 3RD parameter is invalid");
            return;
        }

        // parameters to int
        int analyze = Integer.parseInt(args[0]);

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        args = new String[]{args[1], args[2]};

        switch (analyze) {

            // analyze 0:   calculate total receipt price for each state in descending order
            case 0:
                ReceiptRddAnalyze.main(args);
                break;

            // analyze 1:   calculate average discount percentage for each category in descending order
            case 1:
                ReceiptItemRddAnalyze.main(args);
                break;

            // analyze 2:   calculate total receipt price for each user in descending order
            case 2:
                ReceiptSQLAnalyze.main(args);
                break;
        }

    }

}
