import csv
import os
import sys
from typing import List, Tuple, Iterable, Dict

from whoosh.fields import Schema, TEXT, STORED, ID
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser

from tqdm import tqdm
from whoosh.qparser import QueryParser
from whoosh.qparser import FuzzyTermPlugin
from whoosh.index import open_dir, FileIndex
from whoosh.writing import IndexWriter
import codecs
import pandas as pd


def iter_msmarco_docs() -> Iterable[Dict]:
    with open('../data/msmarco-docs.tsv') as csvfile:
        csv.field_size_limit(sys.maxsize)
        reader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)

        for line in reader:
            doc_id = line[0]
            url = line[1]
            title = line[2]
            body = line[3]

            yield {'doc_id': doc_id, 'url': url, 'title': title, 'body': body}


def create_writer(index: FileIndex) -> IndexWriter:
    w = index.writer(limitmb=4000, procs=4)

    return w


def incremental_index_msmacro(index: FileIndex, commit_every_n: int = 1_000_000):
    indexed_docs = set()

    print('Collecting indexed document IDs...')
    with index.searcher() as searcher:
        for doc in searcher.all_stored_fields():
            indexed_docs.add(doc['doc_id'])

    remaining = 3_200_000 - len(indexed_docs)
    print(f'Found {len(indexed_docs)} documents, adding {remaining} missing documents...')
    writer = create_writer(index)
    i = 0
    for doc in tqdm(iter_msmarco_docs(), total=remaining, unit='docs'):
        if doc['doc_id'] not in indexed_docs:
            writer.add_document(**doc)
            i += 1
            if i % commit_every_n == 0:
                writer.commit()
                writer = create_writer(index)

    writer.commit()

    print('Done!')


def index_msmarco(schema: Schema, dirname: str, num_files: int):
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    ix = create_in(dirname, schema)
    writer = ix.writer()

    with open('../data/msmarco-docs.tsv') as csvfile:
        csv.field_size_limit(sys.maxsize)
        reader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)

        i = 0
        for line in tqdm(reader):
            if i >= num_files:
                break
            doc_id = line[0]
            url = line[1]
            title = line[2]
            body = line[3]

            writer.add_document(doc_id=doc_id, url=url, title=title, body=body)
            i += 1

    writer.commit()


def query(query_str: str, index: FileIndex) -> Iterable[Tuple[str, int, float]]:
    with index.searcher() as searcher:
        query = QueryParser('body', index.schema).parse(query_str)
        results = searcher.search(query)

        for result in results:
            score = result.score
            rank = result.rank
            yield result['doc_id'], rank, score


def generate_results(queries_path: str, index: FileIndex) -> Iterable[Tuple[str, str, int, float]]:
    queries_df = pd.read_csv(queries_path, sep='\t', names=['qid', 'query'])
    for i, row in queries_df.iterrows():
        qid = row['qid']
        query_str = row['query']

        for (document_id, rank, score) in query(query_str, index):
            yield qid, document_id, rank, score


def generate_results_to_file(index: FileIndex, results_filepath, queries_filepath):
    with open(results_filepath, 'w') as results_file:
        for (qid, doc_id, rank, score) in tqdm(generate_results(queries_filepath, index)):
            results_file.write('\t'.join([str(qid), 'Q0', doc_id, str(rank), str(score), 'RUNID']))
            results_file.write('\n')


def main():
    indexdir = 'indexdir'
    if os.path.exists(indexdir):
        index = open_dir(indexdir)
    else:
        schema = Schema(doc_id=ID(unique=True, stored=True), url=ID(unique=True, stored=True),
                        title=TEXT(stored=True), body=TEXT())

        os.mkdir(indexdir)
        index = create_in(indexdir, schema)

    incremental_index_msmacro(index)


if __name__ == '__main__':
    main()
