import networkx as nx
import numpy as np
from sklearn.cluster import SpectralClustering
import pandas as pd 
import umap
import random

def spectral_communities(G,k=2):
    A = nx.adjacency_matrix(G.to_undirected())
    clustering =SpectralClustering(n_clusters=k, eigen_solver=None, affinity='precomputed')
    clusters = clustering.fit(A)
    C = clustering.labels_
    V = [v for v in G.nodes()]
    df_spec = pd.DataFrame({'screen_name':V, 'community':C})
    return df_spec


def umap_layout(G):
    A = nx.adjacency_matrix(G.to_undirected())
    min_dist = 1
    n_neighbors = 25
    fit = umap.UMAP(metric='cosine',min_dist=min_dist,n_neighbors=n_neighbors)
    utransform = fit.fit_transform(A)
    coords = utransform-utransform.mean(axis=0, keepdims=True)  #center the coordinates at zero
    pos = {}
    for cnt,v in enumerate(G.nodes()):
        pos[v] = coords[cnt,:]
    return pos

#network_thinner takes a network G and returns a network Gthin which only has final_frac fraction of edges
def network_thinner(G,final_frac):
	Gthin = G.copy()
	edges = list(Gthin.edges)
	ne_delete = int(G.number_of_edges()*(1-final_frac))
	ebunch = random.sample(edges, ne_delete)
	Gthin.remove_edges_from(ebunch)
	return Gthin