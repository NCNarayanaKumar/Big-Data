package myudfs;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FormatGenre extends EvalFunc <String> {

@Override
    public String exec(Tuple input) {
        try {
            if (input == null || input.size() == 0) {
                return null;
            }

            String str = (String) input.get(0);
            //return str.toUpperCase();
	     String str1 = "";
			if(!str.contains("|"))
			{
				if(str.equals(""))
				{
					str1 = " <lxc122330> ";
					return str1;
				}
				else
				{
					str1 = " " + str1 + " <lxc122330>";
					return str1;
				}
			}
			else
			{
				String arr[] = str.split("\\|");
				
				for( int i = 0; i < arr.length - 1; i++)
				{
					if(i == 0)
					str1 = " " + arr[i];
					else
					str1 = str1 + ", " + arr[i];

				}
				str1 = str1 + " & " + arr[arr.length - 1] + " <lxc122330>";
				return str1;
			
			}
        } catch (ExecException ex) {
            System.out.println("Error: " + ex.toString());
        }

        return null;
    }
}
