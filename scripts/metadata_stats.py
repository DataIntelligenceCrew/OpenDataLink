import sqlite3
import matplotlib.pyplot as plt


def plot(db):
    cur = db.cursor()
    cur.execute("SELECT name, description, attribution, categories, tags "
                "FROM metadata")

    freqs = []
    for row in cur.fetchall():
        score = 0
        for f in row[:3]:
            if f:
                score += 1
        score += len(row[3].split(','))
        score += len(row[4].split(','))
        freqs.append(score)

    fig, ax = plt.subplots()
    ax.hist(freqs, bins=75)
    plt.savefig('metadata.png')


if __name__ == '__main__':
    db = sqlite3.connect('opendatalink.sqlite')
    cur = db.cursor()

    cur.execute("SELECT count(*) FROM metadata")
    total_datasets = cur.fetchone()[0]
    print(f"Number of datasets: {total_datasets}")

    cur.execute("SELECT categories FROM metadata")
    ncat = 0
    for row in cur.fetchall():
        ncat += len(row[0].split(','))
    print(f"Total number of categories: {ncat}")

    cur.execute("SELECT count(*) FROM metadata WHERE description <> ''")
    num_desc = cur.fetchone()[0]
    percent_desc = num_desc / total_datasets * 100
    print(f"Datasets with description: {num_desc} ({percent_desc:.4}%)")

    plot(db)

    db.close()
