package receipts.case_objects;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ReceiptItem {

    String RewardsReceiptId;
    Integer ItemIndex;
    String ReceiptDescription;
    String BarcodeOrig;
    String Barcode;
    Integer Quantity;
    Double ItemPrice;
    Double DiscountedPrice;
    Double Weight;
    String RewardsGroup;
    String Brand;
    String Category;
    String ProductName;

}
