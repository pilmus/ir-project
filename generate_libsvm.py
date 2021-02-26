from typing import List, Iterable, Tuple
import sys
import csv
import random
import os
import mmap
import multiprocessing

from pyserini.index import IndexReader
from pyserini.search import Document
from tqdm import tqdm

import numpy as np


def compute_tf(query_terms: List[str], index_reader: IndexReader, doc_id: str) -> np.ndarray:
    query_tf = np.zeros(len(query_terms))
    doc_vector = index_reader.get_document_vector(doc_id)

    for i, term in enumerate(query_terms):
        query_tf[i] = doc_vector.get(term, 0)

    return query_tf


def compute_idf(query_terms: List[str], index_reader: IndexReader) -> np.ndarray:
    """log ( (|C| - df(term) + 0.5) / (df(term) + 0.5)"""
    C = index_reader.stats()['documents']

    query_idf = np.zeros(len(query_terms))
    for i, term in enumerate(query_terms):
        term_df = index_reader.get_term_counts(term, analyzer=None)[0]

        query_idf[i] = np.log(np.divide(C - term_df + 0.5, term_df + 0.5))
    return query_idf


def compute_tf_idf(query_terms: List[str], index_reader: IndexReader, doc_id: str) -> float:
    tf = compute_tf(query_terms, index_reader, doc_id)
    idf = compute_idf(query_terms, index_reader)
    tf_idfs = np.multiply(tf, idf)

    return tf_idfs


def compute_document_length(index_reader: IndexReader, doc_id: str) -> int:
    return len(index_reader.doc_raw(doc_id))


def compute_bm25(query_terms: List[str], index_reader: IndexReader, doc_id: str, k1=0.9, b=0.4) -> float:
    scores = np.zeros(len(query_terms))
    for i, term in enumerate(query_terms):
        bm25 = index_reader.compute_bm25_term_weight(doc_id, term, analyzer=None, k1=k1, b=b)
        scores[i] = bm25

    return scores

#def lmir_jm(term

# def compute_lmir_jm(query_terms: List[str], index_reader: IndexReader, doc_id: str, sm_param=0.1):
#     doc_vector = index_reader.get_document_vector(doc_id)
#
#
#     # denominator of smoothing constant
#     corpus_prob = 0
#     for term in doc_vector:
#         corpus_prob += index_reader.get_term_counts(term, analyzer=None)[1] / index_reader.stats()['total_terms']
#     corpus_prob = 1 - corpus_prob
#
#     # numerator of smoothing constant
#     doc_prob - 0
#     for term in doc_vector:
#         doc_prob +=
#
#
#     for term in query_terms:
# term_corpus_prob = index_reader.get_term_counts(term, analyzer=None)[1] / index_reader.stats()['total_terms']
#         if term in doc_vector:
#             (1-sm_param) *doc_vector['term'] / len(d.raw()) + sm_param * term_corpus_pr
#             # smoothed_prob(word|document)
#             continue
#         else:
#             # get term collection frequency
#             term_corpus_prob = index_reader.get_term_counts(term, analyzer=None)[1] / index_reader.stats()['total_terms']
#
#
#
#             # constant * prob(term|corpus)
#             continue
#
#     pass


def generate_examples(qrels_path: str,
                      top100_path: str,
                      queries_path: str) -> Iterable[Tuple[str, str, str, bool]]:
    csv.field_size_limit(sys.maxsize)
    stats = {'positive_example_not_in_top100': 0,
             'positive_example_removed_from_top100': 0}

    # query IDs mapped to the queries
    print(f'Loading {queries_path}...')
    queries = {}
    with open(queries_path) as queries_file:
        for line in tqdm(queries_file.readlines()):
            (qid, query) = line.split('\t')
            queries[qid] = query

    # dict of query id mapped to a set of document IDs generated from top100
    print(f'Loading {top100_path}...')
    queries_top100 = {qid: set() for qid in queries.keys()}
    with open(top100_path, 'r') as top100_file:
        for line in tqdm(top100_file.readlines()):
            (qid, _, doc_id, _, _, _) = line.split()
            queries_top100[qid].add(doc_id)

    # dict of query id mapped to a relevant document id
    print(f'Loading {qrels_path}...')
    qrels = {}
    with open(qrels_path) as qrel_file:
        for line in qrel_file.readlines():
            (qid, _, doc_id, _) = line.split()
            qrels[qid] = doc_id

    # remove all positive examples from the top100
    print('Removing positive examples from top100...')
    for qid in tqdm(queries_top100.keys()):
        positive_example = qrels[qid]
        try:
            queries_top100[qid].remove(positive_example)
            stats['positive_example_removed_from_top100'] += 1
        except KeyError:
            stats['positive_example_not_in_top100'] += 1

    print(stats)

    print('Processing examples...')
    for query_id, query in tqdm(queries.items()):
        # yield a positive example for this query
        positive_doc_id = qrels[query_id]
        yield query_id, query, positive_doc_id, True

        # yield a negative example for this query
        negative_doc_id = random.sample(list(queries_top100[qid]), 1)[0]
        yield query_id, query, negative_doc_id, False


def generate_feature_vector(index_reader: IndexReader, query_id: str, query: str, doc_id: str, is_positive: bool) -> str:
    query_terms = index_reader.analyze(query)

    feature_vector = [
        np.sum(compute_tf(query_terms, index_reader, doc_id)),
        np.sum(compute_idf(query_terms, index_reader)),
        np.sum(compute_tf_idf(query_terms, index_reader, doc_id)),
        compute_document_length(index_reader, doc_id),
        np.sum(compute_bm25(query_terms, index_reader, doc_id)),
    ]

    line = [
        '1' if is_positive else '0',
        f'qid:{query_id}',
    ]

    for i, feature in enumerate(feature_vector):
        line.append(f'{i}:{feature}')

    return ' '.join(line) + '\n'


def worker(index_path: str, input_queue: multiprocessing.Queue, output_queue: multiprocessing.Queue):
    print(f'Worker (pid={os.getpid()}) started')
    index_reader = IndexReader(index_path)
    while True:
        try:
            item = input_queue.get(block=True)
            line = generate_feature_vector(index_reader, **item)

            output_queue.put(line)
        except ValueError:
            print(f'Worker (pid={os.getpid()}) stopping')
            break


def main():
    base_path = os.path.join(os.path.dirname(__file__), 'data')
    paths = {
        'qrels_path': os.path.join(base_path, 'msmarco-doctrain-qrels.tsv'),
        'top100_path': os.path.join(base_path, 'msmarco-doctrain-top100'),
        'queries_path': os.path.join(base_path, 'msmarco-doctrain-queries.tsv'),
    }

    index_path = sys.argv[1]

    with open(sys.argv[2], 'w') as output_file:
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        pool = multiprocessing.get_context('spawn').Pool(multiprocessing.cpu_count(), worker, (index_path, input_queue, output_queue))

        print('Generating examples...')
        num_examples = 0
        for (query_id, query, doc_id, is_positive) in tqdm(generate_examples(**paths)):
            input_queue.put((query_id, query, doc_id, is_positive))
            num_examples += 1

        print('Receiving results...')
        for line in tqdm(output_queue.get(block=True)):
            output_file.write(line)
            num_examples -= 1

            if num_examples == 0:
                break

        input_queue.close()
        output_queue.close()
        pool.close()
        pool.join()


if __name__ == '__main__':
    main()
