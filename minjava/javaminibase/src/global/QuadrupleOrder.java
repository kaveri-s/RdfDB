package global;

/**
 * Enumeration class for QuadrupleOrder
 *
 */

public class QuadrupleOrder {

    public static final int SubjectPredicateObjectConfidence = 1;
    public static final int PredicateSubjectObjectConfidence = 2;
    public static final int SubjectConfidence = 3;
    public static final int PredicateConfidence = 4;
    public static final int ObjectConfidence = 5;
    public static final int Confidence = 6;

    public int quadrupleOrder;

    /**
     * QuadrupleOrder Constructor
     *
     * @param quadrupleOrder The possible sorting orderType of the Quadruples
     */

    public QuadrupleOrder (int quadrupleOrder)
    {
        this.quadrupleOrder = quadrupleOrder;
    }

    public String toString()
    {
        switch (quadrupleOrder)
        {
            case SubjectPredicateObjectConfidence:
                return "SubjectPredicateObjectConfidence";
            case PredicateSubjectObjectConfidence:
                return "PredicateSubjectObjectConfidence";
            case SubjectConfidence:
                return "SubjectConfidence";
            case PredicateConfidence:
                return "PredicateConfidence";
            case ObjectConfidence:
                return "ObjectConfidence";
            case Confidence:
                return "Confidence";
        }
        return ("Unexpected QuadrupleOrder " + quadrupleOrder);
    }

}
