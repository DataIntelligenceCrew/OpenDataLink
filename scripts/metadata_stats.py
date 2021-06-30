#!/usr/bin/env python3

import sqlite3
import matplotlib.pyplot as plt


def plot_tag_counts(db, outfile):
    cur = db.cursor()
    cur.execute("SELECT categories, tags FROM metadata")

    counts = []
    for row in cur.fetchall():
        cnt = 0
        for col in row:
            if col:
                cnt += len(col.split(','))
        counts.append(cnt)

    bars = [(None, 1), (None, 2), (None, 3), (None, 4), (None, 5), (None, 6),
            (None, 7), (None, 8), (None, 9), (None, 10), (10, None)]
    names = []
    values = []
    for b in bars:
        if not b[0]:
            names.append(str(b[1]))
            values.append(len([c for c in counts if c == b[1]]))
        elif not b[1]:
            names.append(f">{b[0]}")
            values.append(len([c for c in counts if c > b[0]]))
        else:
            names.append(f"{b[0]}-{b[1]}")
            values.append(len([c for c in counts if c >= b[0] and c <= b[1]]))

    fig, ax = plt.subplots()
    ax.bar(names, values)
    plt.xlabel('Number of tags and categories')
    plt.ylabel('Number of datasets')
    plt.savefig(outfile)


def query_val(db, s):
    return db.execute(s).fetchone()[0]


if __name__ == '__main__':
    db = sqlite3.connect('opendatalink.sqlite')

    print("Data:")

    n = query_val(db, "SELECT count(DISTINCT dataset_id) FROM column_sketches")
    print(f"Number of datasets: {n:,}")

    n = query_val(db, "SELECT count(*) FROM column_sketches")
    print(f"Number of columns: {n:,}")

    print()
    print("Metadata:")

    num_meta = query_val(db, "SELECT count(*) FROM metadata")
    print(f"Number of metadata records: {num_meta:,}")

    n = query_val(db, "SELECT count(*) FROM metadata WHERE name <> ''")
    percent = n / num_meta * 100
    print(f"Datasets with name: {n:,} ({percent:.4}%)")

    n = query_val(db, "SELECT count(*) FROM metadata WHERE description <> ''")
    percent = n / num_meta * 100
    print(f"Datasets with description: {n:,} ({percent:.4}%)")

    n = query_val(db, "SELECT count(*) FROM metadata WHERE attribution <> ''")
    percent = n / num_meta * 100
    print(f"Datasets with attribution: {n:,} ({percent:.4}%)")

    n = query_val(db, "SELECT count(*) FROM metadata WHERE categories <> ''")
    percent = n / num_meta * 100
    print(f"Datasets with categories: {n:,} ({percent:.4}%)")

    n = query_val(db, "SELECT count(*) FROM metadata WHERE tags <> ''")
    percent = n / num_meta * 100
    print(f"Datasets with tags: {n:,} ({percent:.4}%)")

    n = query_val(db, "SELECT count(*) FROM metadata "
                      "WHERE categories <> '' OR tags <> ''")
    percent = n / num_meta * 100
    print(f"Datasets with categories or tags: {n:,} ({percent:.4}%)")

    categories = set()
    for row in db.execute("SELECT categories FROM metadata"):
        categories |= set(row[0].split(','))
    print(f"Total number of categories: {len(categories):,}")

    plot_tag_counts(db, 'tagcounts.png')

    db.close()
