import networkx as nx
import matplotlib.pyplot as plt
import numpy as np


def plot_graph(edges, path_fig=None, take_n=300):
    Gplot = nx.Graph()
    for row in edges.select("src", "dst").take(take_n):
        Gplot.add_edge(row["src"], row["dst"])

    plt.figure(figsize=(15, 10))
    nx.draw(Gplot, with_labels=True, arrows=True)

    if path_fig is not None:
        plt.savefig(path_fig)
    else:
        pass


def plot_n_nodes_vs_degree(x, n_bins, x_step, yscale, ylabel, xlabel, title, save_path=None):
    plt.figure(figsize=(10, 5))
    plt.hist(x, bins=n_bins)
    assert yscale in ["log", "linear"]
    plt.yscale(yscale)
    plt.grid("on")
    plt.xticks(np.arange(min(x) - 1, max(x) + x_step, x_step))
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.title(title)

    if save_path is not None:
        plt.savefig(save_path)
    else:
        pass

    plt.show()
