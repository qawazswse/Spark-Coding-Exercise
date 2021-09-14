package receipts.case_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ReceiptItem implements Serializable {

    private String RewardsReceiptId;
    private Integer ItemIndex;
    private String ReceiptDescription;
    private String BarcodeOrig;
    private String Barcode;
    private Integer Quantity;
    private Double ItemPrice;
    private Double DiscountedPrice;
    private Double Weight;
    private String RewardsGroup;
    private String Brand;
    private String Category;
    private String ProductName;

}
