package quadrupleheap;

/** JAVA */
/**
 * TScan.java-  class TScan
 *
 */

import java.io.*;
import global.*;
import diskmgr.*;
import heap.*;


/**
 * A TScan object is created ONLY through the function openScan
 * of a HeapFile. It supports the getNext interface which will
 * simply retrieve the next record in the heapfile.
 *
 * An object of type scan will always have pinned one directory page
 * of the heapfile.
 */
public class TScan implements GlobalConst{

    /**
     * Note that one record in our way-cool HeapFile implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The heapfile we are using. */
    private QuadrupleHeapFile _qhf;

    /** PageId of current directory page (which is itself an HFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private RID datapageRid = new RID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private THFPage datapage = new THFPage();

    /** record ID of the current record (from the current data page) */
    private QID userqid = new QID();

    /** Status of next user status */
    private boolean nextUserStatus;


    /** The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param qhf A HeapFile object
     */
    public TScan(QuadrupleHeapFile qhf)
            throws InvalidTupleSizeException,
            IOException
    {
        _qhf = qhf;
        firstDataPage();
    }

    /** Retrieve the next record in a sequential scan
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param qid Record ID of the record
     * @return the Quadruple of the retrieved record.
     */
    public Quadruple getNext(QID qid)
            throws InvalidTupleSizeException,
            IOException
    {
        Quadruple recquadruple = null;

        if (nextUserStatus != true) {
            nextDataPage();
        }

        if (datapage == null)
            return null;

        qid.pageNo.pid = userqid.pageNo.pid;
        qid.slotNo = userqid.slotNo;

        try {
            recquadruple = datapage.getRecord(qid);
        }

        catch (Exception e) {
            e.printStackTrace();
        }

        userqid = datapage.nextRecord(qid);
        if(userqid == null) nextUserStatus = false;
        else nextUserStatus = true;

        return recquadruple;
    }


    /** Position the scan cursor to the record with the given qid.
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @param qid Record ID of the given record
     * @return 	true if successful,
     *			false otherwise.
     */
    public boolean position(QID qid)
            throws InvalidTupleSizeException,
            IOException
    {
        QID     nxtqid = new QID();
        boolean bst;

        if (nxtqid.equals(qid)==true)
            return true;

        // This is kind lame, but otherwise it will take all day.
        PageId pgid = new PageId();
        pgid.pid = qid.pageNo.pid;

        if (!datapageId.equals(pgid)) {

            // reset everything and start over from the beginning
            reset();

            bst =  firstDataPage();
            if (bst != true)
                return bst;

            while (!datapageId.equals(pgid)) {
                bst = nextDataPage();
                if (bst != true)
                    return bst;
            }
        }

        // Now we are on the correct page.

        try{
            userqid = datapage.firstRecord();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        if (userqid == null)
        {
            bst = false;
            return bst;
        }

        bst = peekNext(nxtqid);

        while ((bst == true) && (nxtqid != qid))
            bst = mvNext(nxtqid);

        return bst;
    }

    /** Closes the TScan object */
    public void closescan()
    {
        reset();
    }

    /** Reset everything and unpin all pages. */
    private void reset()
    {

        if (datapage != null) {
            try{
                unpinPage(datapageId, false);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null) {
            try{
                unpinPage(dirpageId, false);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        dirpage = null;
        nextUserStatus = true;
    }


    /** Move to the first data page in the file.
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @return true if successful
     *         false otherwise
     */
    private boolean firstDataPage()
            throws InvalidTupleSizeException,
            IOException
    {
        DataPageInfo dpinfo;
        Tuple    rectuple = null;

        /** copy data about first directory page */
        dirpageId.pid = _qhf._firstDirPageId.pid;
        nextUserStatus = true;

        /** get first directory page and pin it */
        try {
            dirpage  = new THFPage();
            pinPage(dirpageId, dirpage, false);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        /** now try to get a pointer to the first datapage */
        datapageRid = dirpage.firstRecord();

        if (datapageRid != null) {
            /** there is a datapage record on the first directory page: */

            try {
                rectuple = dirpage.getRecord(datapageRid);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            dpinfo = new DataPageInfo(rectuple);
            datapageId.pid = dpinfo.pageId.pid;

        } else {

            /** the first directory page is the only one which can possibly remain
             * empty: therefore try to get the next directory page and
             * check it. The next one has to contain a datapage record, unless
             * the heapfile is empty:
             */
            PageId nextDirPageId = dirpage.getNextPage();

            if (nextDirPageId.pid != INVALID_PAGE) {

                try {
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                try {

                    dirpage = new HFPage();
                    pinPage(nextDirPageId, dirpage, false);

                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                /** now try again to read a data record: */

                try {
                    datapageRid = dirpage.firstRecord();
                }

                catch (Exception e) {
                    e.printStackTrace();
                    datapageId.pid = INVALID_PAGE;
                }

                if(datapageRid != null) {

                    try {

                        rectuple = dirpage.getRecord(datapageRid);
                    }

                    catch (Exception e) {
                        //    System.err.println("SCAN: Error getRecord 4: " + e);
                        e.printStackTrace();
                    }

                    if (rectuple.getLength() != DataPageInfo.size)
                        return false;

                    dpinfo = new DataPageInfo(rectuple);
                    datapageId.pid = dpinfo.pageId.pid;

                } else {
                    // heapfile empty
                    datapageId.pid = INVALID_PAGE;
                }
            }//end if01
            else {// heapfile empty
                datapageId.pid = INVALID_PAGE;
            }
        }

        datapage = null;

        try{
            nextDataPage();
        }

        catch (Exception e) {
            //  System.err.println("SCAN Error: 1st_next 0: " + e);
            e.printStackTrace();
        }

        return true;

    }


    /** Move to the next data page in the file and
     * retrieve the next data page.
     *
     * @return 		true if successful
     *			false if unsuccessful
     */
    private boolean nextDataPage()
            throws InvalidTupleSizeException,
            IOException
    {
        boolean nextDataPageStatus;
        DataPageInfo dpinfo;
        PageId nextDirPageId;
        Tuple rectuple = null;

        if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
            return false;

        if (datapage == null) {
            if (datapageId.pid == INVALID_PAGE) {
                // heapfile is empty to begin with

                try{
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e){
                    e.printStackTrace();
                }

            } else {

                // pin first data page
                try {
                    datapage  = new THFPage();
                    pinPage(datapageId, datapage, false);
                }
                catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    userqid = datapage.firstRecord();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return true;
            }
        }

        try{
            unpinPage(datapageId, false /* no dirty */);
            datapage = null;
        }
        catch (Exception e){

        }

        if (dirpage == null) {
            return false;
        }

        datapageRid = dirpage.nextRecord(datapageRid);

        if (datapageRid == null) {
            nextDataPageStatus = false;

            nextDirPageId = dirpage.getNextPage();

            // unpin the current directory page
            try {
                unpinPage(dirpageId, false /* not dirty */);
                dirpage = null;

                datapageId.pid = INVALID_PAGE;
            }

            catch (Exception e) {

            }

            if (nextDirPageId.pid == INVALID_PAGE)
                return false;
            else {

                dirpageId = nextDirPageId;

                try {
                    dirpage  = new THFPage();
                    pinPage(dirpageId, (Page)dirpage, false);
                }

                catch (Exception e){

                }

                if (dirpage == null)
                    return false;

                try {
                    datapageRid = dirpage.firstRecord();
                    nextDataPageStatus = true;
                }
                catch (Exception e){
                    nextDataPageStatus = false;
                    return false;
                }
            }
        }

        try {
            rectuple = dirpage.getRecord(datapageRid);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        if (rectuple.getLength() != DataPageInfo.size)
            return false;

        dpinfo = new DataPageInfo(rectuple);
        datapageId.pid = dpinfo.pageId.pid;

        try {
            datapage = new THFPage();
            pinPage(dpinfo.pageId, datapage, false);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        userqid = datapage.firstRecord();

        if(userqid == null)
        {
            nextUserStatus = false;
            return false;
        }

        return true;
    }

    private boolean peekNext(QID qid) {

        qid.pageNo.pid = userqid.pageNo.pid;
        qid.slotNo = userqid.slotNo;
        return true;

    }


    /** Move to the next record in a sequential scan.
     * Also returns the RID of the (new) current record.
     */
    private boolean mvNext(QID qid)
            throws InvalidTupleSizeException,
            IOException
    {
        QID nextqid;
        boolean status;

        if (datapage == null)
            return false;

        nextqid = datapage.nextRecord(qid);

        if( nextqid != null ){
            userqid.pageNo.pid = nextqid.pageNo.pid;
            userqid.slotNo = nextqid.slotNo;
            return true;
        } else {

            status = nextDataPage();

            if (status==true){
                qid.pageNo.pid = userqid.pageNo.pid;
                qid.slotNo = userqid.slotNo;
            }

        }
        return true;
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     * @see bufmgr.pinPage
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"TScan.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"TScan.java: unpinPage() failed");
        }

    } // end of unpinPage


}
