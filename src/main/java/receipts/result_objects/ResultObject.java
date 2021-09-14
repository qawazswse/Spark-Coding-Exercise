package receipts.result_objects;

import java.io.Serializable;

public interface ResultObject extends Serializable {

    String getJsonString();

    String getParquetString();

    String getCsvString();

    String getString(String fileType);

}
