package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /* page id */
    private PageId pid;
    /* tuple number */
    private int tno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // Done
        this.pid = pid;
        this.tno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // Done
        return tno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // Done
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // Done
        if (!(o instanceof RecordId)) {
            return false;
        }
        RecordId other = (RecordId) o;
        return pid.equals(other.getPageId()) && tno == other.getTupleNumber();
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // Done
        String s = pid.hashCode() + "#" + tno;
        return s.hashCode();
    }

}
