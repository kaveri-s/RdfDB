package iterator;

import java.lang.*;
import chainexception.*;

public class QuadrupleSortException extends ChainException 
{
  public QuadrupleSortException(String s) {super(null,s);}
  public QuadrupleSortException(Exception e, String s) {super(e,s);}
}
