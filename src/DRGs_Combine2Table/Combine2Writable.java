package DRGs_Combine2Table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Combine2Writable implements Writable {
  
	private  Text joinKey ;  // join key
     private Text flag ;  // 文件來源標示
     private Text secondPart ;  // 除了joinkey 的其他部份
     public Combine2Writable(){
    	 this.joinKey  = new Text();
    	 this.flag = new Text();
    	 this.secondPart = new Text();
     }
     public void setJoinKey(Text joinKey){
    	 this.joinKey = joinKey ; 
     }
     public void setFlag(Text flag){
    	 this.flag = flag  ; 
     }
     public void setSecondPart(Text secondPart){
    	 this.secondPart = secondPart ; 
     }
     public Text getFlag(){
    	 return flag;
     }
     public Text getSecondPart(){
    	 return secondPart ; 
     }
     public Text getJoinKey(){
    	 return joinKey ; 
     }
	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
       this.joinKey.readFields(input);
       this.flag.readFields(input);
       this.secondPart.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
      this.joinKey.write(output);
      this.flag.write(output);
      this.secondPart.write(output);
	}
   
	   @Override
		public String toString() {
			// TODO Auto-generated method stub
			return "\t"+this.flag+"\t"+this.joinKey+"\t"+this.secondPart+"\t";
		}

}