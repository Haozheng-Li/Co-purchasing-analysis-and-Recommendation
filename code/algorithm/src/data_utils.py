import pandas as pd
import numpy as np
from tqdm import tqdm
import sys


def read_data(from_file_name):
    df_edges = pd.read_csv(from_file_name, sep="\t", header=None, skiprows=4, names=["src", "dst"])
    vertices_np = \
        np.unique(np.hstack([df_edges["src"].unique(), df_edges["dst"].unique()]))

    df_vertices = pd.DataFrame(data={"id": vertices_np})

    return df_edges, df_vertices


def read_meta(from_file_name):
    if_id = False
    id_ = None
    if_title = False
    title = None
    if_group = False
    group = None
    if_salesrank = False
    salesrank = None
    if_reviews = False
    reviews = None

    def if_all_cols():
        return if_id and if_title and if_group and if_salesrank and if_reviews

    cols = ["id", "title", "group", "salesrank", "reviews"]

    discontinued_prodcut_str = "discontinued product"

    from_file_name = "data/amazon-meta.txt"

    records = []

    with open(from_file_name, "r") as file:
        lines = file.readlines()[2:]

        length = len(lines)
        idx = 0

        for idx in tqdm(range(len(lines)), file=sys.stdout):
            line = lines[idx]

            try:
                if_discontinued_str = lines[idx + 3].strip()

            except Exception:
                break
            else:
                if if_discontinued_str == discontinued_prodcut_str:
                    idx += 4
                else:
                    line_proc = line.strip().lower()

                    line_proc_split = line_proc.split(":")
                    col = line_proc_split[0]
                    if col == "id":
                        id_ = int(line_proc_split[1].strip())
                        if_id = True

                    if col == "title":
                        title = line.split("title:")[1].strip()
                        if_title = True

                    if col == "group":
                        group = line_proc_split[1].strip()
                        if_group = True

                    if col == "salesrank":
                        salesrank = int(line_proc_split[1].strip())
                        if_salesrank = True

                    if col == "reviews":
                        reviews = float(line.split('avg rating:')[-1].strip())
                        if_reviews = True

                    if if_all_cols():
                        records.append({
                            "id": id_,
                            "title": title,
                            "group": group,
                            "salesrank": salesrank,
                            "reviews": reviews
                        })

                        if_id = False
                        id_ = None
                        if_title = False
                        title = None
                        if_group = False
                        group = None
                        if_salesrank = False
                        salesrank = None
                        if_reviews = False
                        reviews = None

                    idx += 1

    return pd.DataFrame(data=records)