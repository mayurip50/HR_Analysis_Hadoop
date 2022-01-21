package SalaryLowDeptwiseMeanSatisfactionAvgWHLeftEmp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MultipleVariableWritable implements Writable{

	Double val1=0.0;
	Double val2=0.0;
	Integer val3=0;
	 public MultipleVariableWritable(double val1,double val2,int val3) {
		this.val1=val1;
		this.val2=val2;
		this.val3=val3; 
	}
	 public MultipleVariableWritable() {
		// TODO Auto-generated constructor stub
	}
	
public void readFields(DataInput in) throws IOException {
	val1=in.readDouble();
	val2=in.readDouble();
	val3=in.readInt();
}
@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(val1);
		out.writeDouble(val2);
		out.writeInt(val3);
	}

	public MultipleVariableWritable merge(MultipleVariableWritable other) {
		this.val1+=other.val1;
		this.val2+=other.val2;
		this.val3+=other.val3;
		return (this);
		
	}

    @Override
    public String toString() {
        return this.val1 + "\t" + this.val2 + "\t" + this.val3;
    }
	
	
}
