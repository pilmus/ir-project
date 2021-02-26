import csv
import random
import gzip
import os
import sys
from collections import defaultdict

# The query string for each topicid is querystring[topicid]
querystring = {}
with gzip.open("data/msmarco-doctrain-queries.tsv.gz", 'rt', encoding='utf8') as f:
    tsvreader = csv.reader(f, delimiter="\t")
    for [topicid, querystring_of_topicid] in tsvreader:
        querystring[topicid] = querystring_of_topicid

# In the corpus tsv, each docid occurs at offset docoffset[docid]
docoffset = {}
with gzip.open("data/msmarco-docs-lookup.tsv.gz", 'rt', encoding='utf8') as f:
    tsvreader = csv.reader(f, delimiter="\t")
    for [docid, _, offset] in tsvreader:
        docoffset[docid] = int(offset)

# For each topicid, the list of positive docids is qrel[topicid]
qrel = {}
with gzip.open("data/msmarco-doctrain-qrels.tsv.gz", 'rt', encoding='utf8') as f:
    tsvreader = csv.reader(f, delimiter=" ")
    for [topicid, _, docid, rel] in tsvreader:
        assert rel == "1"
        if topicid in qrel:
            qrel[topicid].append(docid)
        else:
            qrel[topicid] = [docid]


def getcontent(docid, f):
    """getcontent(docid, f) will get content for a given docid (a string) from filehandle f.
    The content has four tab-separated strings: docid, url, title, body.
    """

    f.seek(docoffset[docid])
    line = f.readline()
    assert line.startswith(docid + "\t"), \
        f"Looking for {docid}, found {line}"
    return line.rstrip()


def generate_triples(outfile, triples_to_generate):
    """Generates triples comprising:
    - Query: The current topicid and query string
    - Pos: One of the positively-judged documents for this query
    - Rnd: Any of the top-100 documents for this query other than Pos

    Since we have the URL, title and body of each document, this gives us ten columns in total:
    topicid, query, posdocid, posurl, postitle, posbody, rnddocid, rndurl, rndtitle, rndbody

    outfile: The filename where the triples are written
    triples_to_generate: How many triples to generate
    """

    stats = defaultdict(int)
    unjudged_rank_to_keep = random.randint(1, 100)
    already_done_a_triple_for_topicid = -1

    with gzip.open("data/msmarco-doctrain-top100.gz", 'rt', encoding='utf8') as top100f, \
            open(outfile, 'w', encoding="utf8", newline='') as out:
        outwriter = csv.writer(out, delimiter=' ')
        for line in top100f:
            [topicid, _, unjudged_docid, rank, _, _] = line.split()

            if already_done_a_triple_for_topicid == topicid or int(rank) != unjudged_rank_to_keep:
                stats['skipped'] += 1
                continue
            else:
                unjudged_rank_to_keep = random.randint(1, 100)
                already_done_a_triple_for_topicid = topicid

            assert topicid in querystring
            assert topicid in qrel
            assert unjudged_docid in docoffset

            # Use topicid to get our positive_docid
            positive_docid = random.choice(qrel[topicid])
            assert positive_docid in docoffset

            if unjudged_docid in qrel[topicid]:
                stats['docid_collision'] += 1
                continue

            stats['kept'] += 1

            # Each line has 3 columns: topicid, positive docid, unjudged docid
            outwriter.writerow([topicid, 0, unjudged_docid, 0])

            triples_to_generate -= 1
            if triples_to_generate <= 0:
                return stats


stats = generate_triples("data/msmarco-doctrain-qrels.tsv/msmarco-doctrain-negative-qrels.tsv", 1000)

for key, val in stats.items():
    print(f"{key}\t{val}")