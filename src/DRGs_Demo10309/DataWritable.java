package DRGs_Demo10309;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataWritable implements Writable {

	private String ID;
	private long HealthyCareFee;
	private long SinglePrice;
	private String ItemName;
   private String index ;
	public DataWritable() {
		super();
	}

	public DataWritable(String index,String ID, long HealthyCareFee, long SinglePrice,
			String ItemName) {
		super();
		this.index = index ;
		this.ID = ID;
		this.HealthyCareFee = HealthyCareFee;
		this.SinglePrice = SinglePrice;
		this.ItemName = ItemName;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(index);
		out.writeUTF(ID);
		out.writeLong(HealthyCareFee);
		out.writeLong(SinglePrice);
		out.writeUTF(ItemName);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		index =  in.readUTF();
		ID = in.readUTF();
		HealthyCareFee = in.readLong();
		SinglePrice = in.readLong();
		ItemName = in.readUTF();
	}
    public String getIndex(){
    	return index;
    }

	public String getTotalFee() {
		return ID;
	}
  public void setIndex(String index){
	  this.index = index;
  }
	public void setTotalFee(String ID) {
		this.ID = ID;
	}

	public long getHealthyCareFee() {
		return HealthyCareFee;
	}

	public void setHealthyCareFee(long HealthyCareFee) {
		this.HealthyCareFee = HealthyCareFee;
	}

	public long getSinglePrice() {
		return SinglePrice;
	}

	public void setSinglePrice(long SinglePrice) {
		this.SinglePrice = SinglePrice;
	}

	public void setItemName (String ItemName){
		this.ItemName = ItemName; 
	}
	public String getItemName(){
		return ItemName; 
	}
	@Override
	public String toString() {
		return ";"+index+";" + ID + ";" + HealthyCareFee + ";" + SinglePrice +";"+ItemName+";";
	}
}