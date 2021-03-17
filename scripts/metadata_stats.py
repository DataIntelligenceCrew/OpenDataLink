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

    bars = [(None, 0), (None, 1), (None, 2), (None, 3), (None, 4), (None, 5),
            (None, 6), (None, 7), (None, 8), (None, 9), (None, 10), (10, None)]
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


if __name__ == '__main__':
    db = sqlite3.connect('opendatalink.sqlite')
    cur = db.cursor()

    cur.execute("SELECT count(*) FROM metadata")
    num_datasets = cur.fetchone()[0]
    print(f"Number of datasets: {num_datasets:,}")

    cur.execute("SELECT count(*) FROM column_sketches")
    num_columns = cur.fetchone()[0]
    print(f"Number of columns: {num_columns:,}")

    cur.execute("SELECT categories FROM metadata")
    categories = set()
    for row in cur.fetchall():
        categories |= set(row[0].split(','))
    print(f"Total number of categories: {len(categories):,}")

    cur.execute("SELECT count(*) FROM metadata WHERE description <> ''")
    num_desc = cur.fetchone()[0]
    percent_desc = num_desc / num_datasets * 100
    print(f"Datasets with description: {num_desc:,} ({percent_desc:.4}%)")

    plot_tag_counts(db, 'tagcounts.png')

    db.close()
