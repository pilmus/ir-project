import csv
import os
import sys

from whoosh.fields import Schema, TEXT, STORED, ID
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser

from tqdm import tqdm
from whoosh.qparser import QueryParser
from whoosh.qparser import FuzzyTermPlugin
from whoosh.index import open_dir
import codecs


def index_msmarco(schema: Schema, dirname: str, num_files: int):
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    ix = create_in(dirname, schema)
    writer = ix.writer()

    with open('data/msmarco-docs.tsv') as csvfile:
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


def main():
    schema = Schema(doc_id=ID(unique=True, stored=True), url=ID(unique=True, stored=True),
                    title=TEXT(stored=True), body=TEXT)
    index_msmarco(schema, 'indexdir_small', 10000)

    # ix = open_dir("indexdir")
    #
    # with ix.searcher() as searcher:
    #     query = QueryParser('body', ix.schema).parse('animals')
    #     results = searcher.search(query)
    #
    #     results.fragmenter.charlimit = 100000
    #
    #     for r in results:
    #         print(r)
    #
    #     print(len(results))
    #     print(results[0].items())


if __name__ == '__main__':
    main()
