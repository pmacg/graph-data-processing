# Graph Data Processing
Various tools for processing graph data.

## Working with data
You should store the datasets you are working on in a directory named `data`. This directory will be ignored by git and so will not be checked into the repository.

## Standard output format
In order to make using these datasets as straightforward as possible, we use a standard output
format for the network data. The goal of this repository is to provide the code to wrangle the data
into this standard format. You can then write your downstream code assuming that the network data arrives in this format.

The format we use is based on simple text files. This keeps everything quite simple and allows for
fast processing of the data. The main disadvantage is that we might lose some information. For
example, there is no difference between an edgelist file for a directed graph and for the equivalent
undirected graph. The user of the data must be aware of this and write their downstream code
accordingly.

For each dataset, we produce some subset of the following output files.
- `<network_name>.edgelist`: the main file describing the graph. Nodes are indexed numerically
  starting at `1`. Each line corresponds to an edge in the graph, and contains a space-seperated
  list of vertices. For **directed graphs**, the edge direction is from the first node to the second
  node. For **hypergraphs**, each line is a list of arbitrary length, describing a single hyperede.
- `<network_name>.edges`: each line contains a label which corresponds to an edge in the graph. The
  order of the edges is the same as the `edgelist` file.
- `<network_name>.vertices`: each line contains a label which corresponds to a vertex in the graph.
  The vertices are listed in order of their numerical index. Note that this is **not** the cluster
  label of the vertex, but the individual name of the vertex.
- `<network_name>.gt`: each line gives a cluster index (starting with `1`) for each vertex.
- `<network_name>.clusters`: each line gives the name of the corresponding cluster.
