package DRGs_Demo10309;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DRGsWritable implements Writable {
	long TotalHostpitaledFee = 0;
	long HealthyCareFee = 0;
	long TotalSelfPayFee = 0;
	long  Count = 0 ; 

	public DRGsWritable() {
		super();
	}

	public DRGsWritable(long TotalHostpitaledFee, long HealthyCareFee,
			long TotalSelfPayFee,long Count) {
		super();
		this.TotalHostpitaledFee = TotalHostpitaledFee;
		this.HealthyCareFee = HealthyCareFee;
		this.TotalSelfPayFee = TotalSelfPayFee;
		this.Count = Count;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		TotalHostpitaledFee = input.readLong();
		HealthyCareFee = input.readLong();
		TotalSelfPayFee = input.readLong();
		Count = input.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

		out.writeLong(TotalHostpitaledFee);
		out.writeLong(HealthyCareFee);
		out.writeLong(TotalSelfPayFee);
		out.writeLong(Count);
	}
	public long getTotalHostpitaledFee() {
		return TotalHostpitaledFee;
	}

	public void setTotalHostpitaledFee(long TotalHostpitaledFee) {
		this.TotalHostpitaledFee = TotalHostpitaledFee;
	}
	
	public long getHealthyCareFee() {
		return HealthyCareFee;
	}

	public void setHealthyCareFee(long HealthyCareFee) {
		this.HealthyCareFee = HealthyCareFee;
	}
	public long getCount() {
		return Count;
	}

	public void setCount(long Count) {
		this.Count = Count;
	}
	public long getTotalSelfPayFee() {
		return TotalSelfPayFee;
	}

	public void setTotalSelfPayFee(long TotalSelfPayFee) {
		this.TotalSelfPayFee = TotalSelfPayFee;
	}
	public String toString() {
		return  "\t" + TotalHostpitaledFee + "\t" + HealthyCareFee +"\t"+TotalSelfPayFee+"\t"+Count;
	}
}
