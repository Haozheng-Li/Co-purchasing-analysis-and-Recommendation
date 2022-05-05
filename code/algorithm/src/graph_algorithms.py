from pyspark.sql.functions import desc


class LabelPropagationAlgorithm():
    def __init__(self, g, max_iter):
        self.max_iter = max_iter
        self.communities = g.labelPropagation(maxIter=self.max_iter)
        print("LPA has been fit to graph")

    def get_communities(self, ):
        return self.communities.persist()

    def __call__(self, id_):
        id_label = self.communities.filter(f"id == {id_}").select("label").collect()[0]["label"]
        recommendations = \
            self.communities.filter(f"label == {id_label}").\
            filter(f"id != {id_}").\
            sort(desc("reviews"))
        return recommendations


class JaccardSimilarity():
    def __init__(self, g, id_, spark):
        # get neighbours of node
        self.g = g
        self.id_ = id_
        self.spark = spark

        ids_neighbour = self.get_neighbour_ids(g, id_)

        similarities = []

        for id_neighbour in ids_neighbour:
            # get neighbours of neighbour node
            ids_neighbours_of_neighbour = self.get_neighbour_ids(g, id_neighbour)
            js = self.jaccard_sim(ids_neighbour, ids_neighbours_of_neighbour)
            similarities.append((id_neighbour, js))

        self.similarities_df = self.spark.createDataFrame(similarities, ["id", "jaccard"])

    def get_neighbour_ids(self, g, id_):
        triplets = g.triplets.filter(f"src.id == {id_}")
        return set([row["dst"]["id"] for row in triplets.collect()])

    def jaccard_sim(self, a, b):
        return len(a.intersection(b)) / len(a.union(b))

    def __call__(self):
        return self.g.vertices. \
            join(self.similarities_df, self.g.vertices["id"] == self.similarities_df["id"], "inner"). \
            drop(self.similarities_df.id). \
            sort(desc("jaccard"), desc("reviews"))
