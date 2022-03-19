package iterator;


import heap.*;
import global.*;
import java.io.*;
import java.lang.*;
import labelheap.*;

/**
 *some useful method when processing Label 
 */
public class LabelUtils
{
  
  /**
   * This function compares a label with another label
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the label is greater,
   *   -1        if the label is smaller,
   *
   *@param    l1        one quadruple.
   *@param    l2        another quadruple.
   *@return   0        if the two are equal,
   *          1        if the quadruple is greater,
   *         -1        if the quadruple is smaller,                              
   *
   */
  public static int CompareLabelWithLabel(
    Label l1,
    Label l2)
    {
      String l1_s = l1.getLabel();
      String l2_s = l2.getLabel();
      // Now handle the special case that is posed by the max_values for strings...
      if (l1_s.compareTo(l2_s) > 0)
      {
        return 1;
      }
      if (l1_s.compareTo(l2_s) < 0)
      {
        return -1;
      }
      return 0;
    }

}




