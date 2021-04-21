# This file includes methods for processing the foodweb dataset available here
#   https://snap.stanford.edu/data/Florida-bay.html
# and described in
#   Robert E. Ulanowicz, Cristina Bondavalli, and Michael S. Egnotovich, 1997, Network analysis of
#   trophic dynamics in South Florida ecosystems--The Florida Bay Ecosystem: Annual Report to the
#   U.S. Geological Survey, Biological Resources Division.
# and
#   Austin R. Benson, David F. Gleich, and Jure Leskovec.
#   "Higher-order Organization of Complex Networks." (2016). Science, 353.6295 (2016): 163–166.
using CSV
using DataFrames

META_FILENAME = "Florida-bay-meta.csv"
EDGELIST_FILENAME = "Florida-bay.txt"


"""
  writeFoodwebNetwork(foodwebDirectory)

Write the foodweb data files to the given directory.

The supplied directory must contain the extracted foodweb data files from
https://snap.stanford.edu/data/Florida-bay.html.

We drop vertices which are labeled detritus.
"""
function writeFoodwebNetwork(foodwebDirectory::String)
    # Start by writing the edgeslist file. This is quite straightforward. We will remove the
    # commented lines from the original file which might be unnecessary, but it could simplify
    # downstream processing.
    input_filename = joinpath(foodwebDirectory, EDGELIST_FILENAME)
    output_filename = joinpath(foodwebDirectory, "foodweb.edgelist")

    # Hardcode the detritus nodes. It turns out we can just ignore those above 124.
    detritus_nodes_above = 123

    edgelist_df = DataFrame(CSV.File(input_filename, comment="#", header=false))

    # We hardcode some vertices to ignore
    n = 128
    ignore_vertices = [12, 123, 124, 126]

    # Figure out how to correct each node index once we've ignored those above
    index_corrections = Dict()
    current_correction = 0
    for index = 1:n
        index_corrections[index] = index - current_correction
        if index in ignore_vertices
            current_correction += 1
        end
    end

    open(output_filename, "w") do f_out
        for row in eachrow(edgelist_df)
            if (row[1] + 1) ∉ ignore_vertices && (row[2] + 1) ∉ ignore_vertices
                write(f_out, repr(index_corrections[row[1] + 1]), " ",
                      repr(index_corrections[row[2] + 1]), "\n")
            end
        end
    end

    # Now, lets read in the rest of the data, and see what we can do with it
    input_filename = joinpath(foodwebDirectory, META_FILENAME)
    meta_df = DataFrame(CSV.File(input_filename))

    # The first thing we can do is save the vertex names
    output_filename = joinpath(foodwebDirectory, "foodweb.vertices")
    open(output_filename, "w") do f_out
        for row in eachrow(meta_df)
            if (row["node_id"] + 1) ∉ ignore_vertices
                write(f_out, row["name"], "\n")
            end
        end
    end

    # We can now construct a list of cluster names, and then a dictionary mapping them to individual
    # indices.
    cluster_names = unique(meta_df[:, "group"])
    filter!(e -> e !== missing, cluster_names)
    cluster_dictionary = Dict(cluster => index for (index, cluster) in enumerate(cluster_names))

    # Now, we can write the ground truth clusters file
    output_filename = joinpath(foodwebDirectory, "foodweb.gt")
    open(output_filename, "w") do f_out
        for row in eachrow(meta_df)
            if (row["node_id"] + 1) ∉ ignore_vertices
                if row["group"] === missing
                    write(f_out, "0\n")
                else
                    write(f_out, repr(cluster_dictionary[row["group"]]), "\n")
                end
            end
        end
    end

    # And finally, we can write the cluster labels file
    output_filename = joinpath(foodwebDirectory, "foodweb.clusters")
    open(output_filename, "w") do f_out
        for name in cluster_names
            write(f_out, name, "\n")
        end
    end
end

"""
  writeFoodwebHypergraph(foodwebDirectory)

Construct the motif-based hypergraph from the foodweb network.

This constructed hypergraph has been used in the following papers.

Li, Pan, and Olgica Milenkovic. "Inhomogeneous Hypergraph Clustering with Applications." NIPS. 2017.

Fountoulakis, Kimon, Pan Li, and Shenghao Yang. "Local Hyper-flow Diffusion."
arXiv preprint arXiv:2102.07945 (2021).
"""
function writeFoodwebHypergraph(foodwebDirectory::String)
    # Start by writing the ordinary graph information.
    writeFoodwebNetwork(foodwebDirectory)

    # Construct two edgelists - one of 'out' edges and one of 'in' edges
    out_edgelist = Dict()
    in_edgelist = Dict()
    edges_df = DataFrame(CSV.File(joinpath(foodwebDirectory, "foodweb.edgelist"), header=false))
    for row in eachrow(edges_df)
        # Add this edge to the out-edge dict
        if row[1] ∈ keys(out_edgelist)
            if row[2] ∉ out_edgelist[row[1]]
                push!(out_edgelist[row[1]], row[2])
            end
        else
            out_edgelist[row[1]] = [row[2]]
        end

        # Add this edge to the in-edge dict
        if row[2] ∈ keys(in_edgelist)
            if row[1] ∉ in_edgelist[row[2]]
                push!(in_edgelist[row[2]], row[1])
            end
        else
            in_edgelist[row[2]] = [row[1]]
        end
    end

    # We will now add a single extra file with the hypergraph edgelist.
    output_filename = joinpath(foodwebDirectory, "foodweb_hypergraph.edgelist")
    open(output_filename, "w") do f_out
        # To find all of the motifs, we first loop over all vertices, checking if this node is prey
        for prey1 in 1:max(maximum(keys(out_edgelist)), maximum(keys(in_edgelist)))
            if !haskey(out_edgelist, prey1)
                continue
            end
            # Check each pair of 'out' edges - let these be the predators
            for pred1_idx in 1:length(out_edgelist[prey1])
                for pred2_idx in (pred1_idx + 1):length(out_edgelist[prey1])
                    predator1 = out_edgelist[prey1][pred1_idx]
                    predator2 = out_edgelist[prey1][pred2_idx]

                    # Now, look for a second prey node which has the same predators.
                    for prey2 in (prey1 + 1):max(maximum(keys(out_edgelist)),
                                                 maximum(keys(in_edgelist)))
                        if haskey(out_edgelist, prey2)
                            if predator1 ∈ out_edgelist[prey2] && predator2 ∈ out_edgelist[prey2]
                                # We have found a motif!
                                write(f_out, repr(prey1), " ", repr(prey2), " ", repr(predator1),
                                     " ", repr(predator2), "\n")
                            end
                        end
                    end
                end
            end
        end
    end
end
