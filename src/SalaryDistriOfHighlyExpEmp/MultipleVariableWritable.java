package SalaryDistriOfHighlyExpEmp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MultipleVariableWritable implements Writable{

	Integer val1=0;
	String val2="";
	 public MultipleVariableWritable(int val1,String val2) {
		this.val1=val1;
		this.val2=val2;
		
	}
	 public MultipleVariableWritable() {
		// TODO Auto-generated constructor stub
	}
	
public void readFields(DataInput in) throws IOException {
	val1=in.readInt();
	val2=in.readLine();

}
@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(val1);
		out.writeChars(val2);

	}

	public MultipleVariableWritable merge(MultipleVariableWritable other) {
		this.val1+=other.val1;
		this.val2+=other.val2;
		
		return (this);
		
	}

    @Override
    public String toString() {
        return this.val1 + "\t" + this.val2 ;
    }
	
	
}
