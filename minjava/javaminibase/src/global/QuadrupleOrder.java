package global;

/**
 * Enumeration class for QuadrupleOrder
 *
 */

public class QuadrupleOrder {

    public static final int Field_default                           = 0;
    public static final int SubjectPredicateObjectConfidence        = 1;
    public static final int PredicateSubjectObjectConfidence        = 2;
    public static final int SubjectConfidence                       = 3;
    public static final int PredicateConfidence                     = 4;
    public static final int ObjectConfidence                        = 5;
    public static final int Confidence                              = 6;
    public static final int Subject                                 = 7;

    public int _sortBy;

    public static final int Sort_default                            = 0;
    public static final int Ascending                               = 1;
    public static final int Descending                              = 2;

    public int _sortingOrder;

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

    public QuadrupleOrder (int sortBy) {
        _sortBy = sortBy;
        _sortingOrder = Ascending;
    }

    public QuadrupleOrder (int sortBy, int sortingOrder) {
        _sortBy = sortBy;
        _sortingOrder = sortingOrder;
    }

    public static QuadrupleOrder getSortOrder(int sortBy) {
        return getSortOrder(sortBy, Ascending);
    }

    public static QuadrupleOrder getSortOrder(int sortBy, int sortingOrder) {

        QuadrupleOrder quadrupleOrder = null;

        if (sortBy >= 1 && sortBy <= 7 && sortingOrder >= 1 && sortingOrder <= 2)
        {
            quadrupleOrder = new QuadrupleOrder(sortBy, sortingOrder);
        }
        else
        {
            System.err.println("RuntimeError. Sort order out of range (1-7). Welp, shouldn't be here");
            System.exit(1);
        }

        return quadrupleOrder;
    }
}