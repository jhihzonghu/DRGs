package DRGs_specialCatTable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PCN implements Writable {

     private String PCN ; 
     private String Part2;
	public String toString() {
		// TODO Auto-generated method stub
		return Part2+","+PCN;
	}

	
	public PCN()
	{
		
	}
	public PCN(String Part2, String PCN) {
		// TODO Auto-generated constructor stub
		this.PCN = PCN ; 
		this.Part2 = Part2; 
	}
  public void setPCN(String PCN)
  {
	this.PCN = PCN ;  
  }	

  public String getPCN()
  {
	  return PCN ; 
  }
  public void setPart2(String Part2)
  {
		this.Part2 = Part2;   
  }	

  public String getPart2()
  {
	  return Part2 ; 
  }
	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
      PCN = input.readUTF() ; 
      Part2 = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
    output.writeUTF(PCN);
    output.writeUTF(Part2);
	}

}
