import csv
import gzip
import itertools
from collections import Counter

from pyserini.index import IndexReader
from tqdm import tqdm


def generate_querystring(query_zip: str) -> dict:
    # The query string for each topicid is querystring[topicid]
    print(f"Generating queries from {query_zip}")
    querystring = {}
    with gzip.open(query_zip, 'rt', encoding='utf8') as f:
        tsvreader = csv.reader(f, delimiter="\t")
        for [topicid, querystring_of_topicid] in tsvreader:
            querystring[topicid] = querystring_of_topicid
    return querystring


def generate_doc_offset(offset_zip: str) -> dict:
    # In the corpus tsv, each docid occurs at offset docoffset[docid]
    print(f"Generating doc offsets from {offset_zip}")
    docoffset = {}
    with gzip.open(offset_zip, 'rt', encoding='utf8') as f:
        tsvreader = csv.reader(f, delimiter="\t")
        for [docid, _, offset] in tsvreader:
            docoffset[docid] = int(offset)
    return docoffset


def generate_qrel_dict(qrel_zip: str) -> dict:
    """
    For each query id, extract the list of associated relevant and irrelevant documents.
    :param qrel_zip:
    :return: a dictionary where key := query id, val := [(doc, rel))]
    """
    # For each topicid, the list of positive docids is qrel[topicid]
    print(f"Generating qrels from {qrel_zip}")

    qrel = {}
    with gzip.open(qrel_zip, 'rt', encoding='utf8') as f:
        tsvreader = csv.reader(f, delimiter=" ")
        for [topicid, _, docid, rel] in tsvreader:
            rel = int(rel)
            if topicid not in qrel:
                qrel[topicid] = []
            qrel[topicid].append((docid, rel))
    return qrel


def compute_doc_tf(index_reader, query, document_id) -> int:
    val = 0
    query_terms = index_reader.analyze(query)
    doc_vector = index_reader.get_document_vector(document_id)


    for term in query_terms:
        tf = doc_vector.get(term, 0)
        val += tf
    return val


def compute_tf(index_reader, query, string):
    val = 0
    query_terms = index_reader.analyze(query)
    string_terms = index_reader.analyze(string)
    string_terms_counts = Counter(string_terms)
    for term in query_terms:
        val += string_terms_counts.get(term, 0)
    return val


def generate_libsvm_representation(index_reader, queries_zip, qrels_zip, libsvm_file, num_queries=10) -> None:
    queries = generate_querystring(queries_zip)
    qrels = generate_qrel_dict(qrels_zip)

    with open(libsvm_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=' ')
        for query_id in tqdm(itertools.islice(qrels, num_queries)):
            query = queries[query_id]
            doc_ids = qrels.get(query_id, [])
            for doc_id, rel in tqdm(doc_ids):
                doc_tf = compute_doc_tf(index_reader, query, doc_id)
                doc_len = len(index_reader.doc(doc_id).raw())
                csvwriter.writerow([rel, f"qid:{query_id}", f"1:{doc_tf}", f"2:{doc_len}", f"#DOCID:{doc_id}"])


def main():
    index_reader = IndexReader("../anserini/indexes/msmarco-doc/lucene-index-msmarco")
    generate_libsvm_representation(index_reader, "data/msmarco-doctrain-queries.tsv.gz",
                                   "data/msmarco-doctrain-qrels.tsv.gz",
                                   "data/msmarco-doc-libsvm/msmarco-doctrain-libsvm.txt", num_queries=100)
    # generate_libsvm_representation(index_reader, "data/msmarco-test2019-queries.tsv.gz",
    #                                "data/2019qrels-docs.txt.gz",
    #                                "data/msmarco-doc-libsvm/msmarco-doctest-libsvm.txt", num_queries=10)
    generate_libsvm_representation(index_reader, "data/msmarco-docdev-queries.tsv.gz",
                                   "data/msmarco-docdev-qrels.tsv.gz",
                                   "data/msmarco-doc-libsvm/msmarco-docdev-libsvm.txt", num_queries=100)


if __name__ == '__main__':
    main()
