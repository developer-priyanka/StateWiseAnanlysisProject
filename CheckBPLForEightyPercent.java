package apache_pig_udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CheckBPLForEightyPercent extends EvalFunc<String> {
	//check the districts who have reached 80% of objectives of BPL Card 

	@Override
	public String exec(Tuple arg0) throws IOException {
		
		if(arg0==null|| arg0.size()==0)      //checking if inputs are null or 0.
			return "";
		int numofinputs=arg0.size();
		if(numofinputs<2)
			return "";  
		long x=Long.parseLong((String)arg0.get(0)); //objective of BPL Crad
		long y=Long.parseLong((String)arg0.get(1)); //performance on BPL Card
		long z=(long)(x*80)/100; //calculating 80% of BPL objectives
		
		if(y>=z) //compare objectives with performance
			return "Yes";
		
		
		return "NO";
	}
	
}

