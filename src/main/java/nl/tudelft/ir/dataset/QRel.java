package nl.tudelft.ir.dataset;

public class QRel {
    private final String qid;
    private final String docId;
    private final String label;

    public QRel(String qid, String docId, String label) {
        this.qid = qid;
        this.docId = docId;
        this.label = label;
    }

    public String getQid() {
        return qid;
    }

    public String getDocId() {
        return docId;
    }

    public String getLabel() {
        return label;
    }
}
