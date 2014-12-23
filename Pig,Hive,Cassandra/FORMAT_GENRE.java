import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
 
public final class FormatGenre extends UDF 
{
  public Text evaluate(final Text s) 
  {
	try
	{
		String str = s.toString();
		if (str == null) 
		{ 
		return null; 
		}

		String str1 = "";
	
		if(!str.contains("|"))
			{
				if(str.equals(""))
				{
					str1 = " ";
					return new Text(str1);
				}
				else
				{
					str1 = " " + str;
					return new Text(str1);
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
				return new Text(str1);
			}
			
	} catch (Exception ex) {
            System.out.println("Error: " + ex.toString());
        }
	return null;		
  }
}