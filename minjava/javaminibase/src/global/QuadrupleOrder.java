package global;

/** 
 * Enumeration class for QuadrupleOrder
 * 
 */

public class QuadrupleOrder {

  public static final int Default                 = 0;
  public static final int SubjectConfidence       = 1;
  public static final int PredicateConfidence     = 2;
  public static final int ObjectConfidence        = 3;
  public static final int Confidence              = 4;
  public static final int Subject                 = 5;

  public int quadrupleOrder;

  /** 
   * QuadrupleOrder Constructor
   * <br>
   * A quadruple ordering can be defined as 
   * <ul>
   * <li>   QuadrupleOrder quadrupleOrder = new QuadrupleOrder(QuadrupleOrder.SubjectConfidence);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (quadrupleOrder.quadrupleOrder == QuadrupleOrder.SubjectConfidence) ....
   * </ul>
   *
   * @param _quadrupleOrder The possible ordering of the quadruples 
   */

  public QuadrupleOrder (int _quadrupleOrder) {
    quadrupleOrder = _quadrupleOrder;
  }

  public static QuadrupleOrder getSortOrder(int indexOption) {
    
    QuadrupleOrder sort_order = null;
    switch(indexOption)
    {
        case 1:
        sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectConfidence);
        break;
        case 2:
        sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateConfidence);
        break;
        case 3:
        sort_order = new QuadrupleOrder(QuadrupleOrder.ObjectConfidence);
        break;
        case 4:
        sort_order = new QuadrupleOrder(QuadrupleOrder.Confidence);
        break;
        case 5:
        sort_order = new QuadrupleOrder(QuadrupleOrder.Subject);
        break;
        default:
        System.err.println("RuntimeError. Sort order out of range (1-5). Welp, shouldn't be here");
        System.exit(1);
        break;
    }
    return sort_order;
  }

}
