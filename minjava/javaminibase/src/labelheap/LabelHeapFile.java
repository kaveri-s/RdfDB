package labelheap;

import global.LID;

import java.lang.String;

public class LabelHeapFile {

    public LabelHeapFile (String name) {

    }

    void deleteFile() {

    }

    boolean deleteLabel(LID lid) {
        return false;
    }

    int getLabelCnt() {
        return 0;
    }

    String getLabel(LID lid) {
        return null;
    }

    LID insertLabel(String Label) {
        return new LID();
    }

    LScan openScan() {
        return new LScan();
    }

    boolean updateLabel(LID lid, String newLabel) {
        return false;
    }
}
