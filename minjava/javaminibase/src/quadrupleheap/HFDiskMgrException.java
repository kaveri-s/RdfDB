/* File hferr.java  */

package quadrupleheap;
import chainexception.*;

public class HFDiskMgrException extends ChainException{


  public HFDiskMgrException()
  {
     super();
  
  }

  public HFDiskMgrException(Exception ex, String name)
  {
    super(ex, name);
  }



}
