# Include some methods for processing the IMDB dataset. The dataset should be available at
#   https://www.imdb.com/interfaces/
# These methods assume that the data has been downloaded and extracted. You should pass the folder
# containing these files to the methods for processing them.
using CSV
using DataFrames

TITLE_BASICS_FILENAME = "title.basics.tsv"
NAME_BASICS_FILENAME = "name.basics.tsv"
CREDITS_FILENAME = "title.principals.tsv"

struct Imdb
    # film ID -> film Title
    films::Dict{String, String}

    # actor ID -> actor name
    actors::Dict{String, String}

    # actor ID -> cluster ID
    clusters::Dict{String, Int64}

    # film ID -> array of credited people's IDs
    credits::Dict{String, Array{String}}
end

"""
    loadimdb(directory)

Load the imdb dataset into memory, returning an Imdb object.
"""
function loadimdb(directory)
    # First, we will construct the films dictionary.
    #
    # Select the 'movies' from the titles file, and store the ID with the Movie title.
    filename = joinpath(directory, TITLE_BASICS_FILENAME)
    df = DataFrame(CSV.File(filename, select=[:tconst, :titleType, :primaryTitle]))
    moviesdf = df[df.titleType .== "movie", :]
    films = Dict(Pair.(moviesdf.tconst, moviesdf.primaryTitle))

    # Clear the dataframe to free up memory
    moviesdf = nothing

    # Now, by a similar method, we construct the actors dictionary.
    filename = joinpath(directory, NAME_BASICS_FILENAME)
    df = DataFrame(CSV.File(filename, select=[:nconst, :primaryName]))
    actors = Dict(Pair.(df.nconst, df.primaryName))

    # Now, construct the credits dictionary
    filename = joinpath(directory, CREDITS_FILENAME)
    df = DataFrame(CSV.File(filename, select=[:tconst, :nconst, :category]))

    # Select only the rows which correspond to films in our film dictionary.
    df = df[in(keys(films)).(df.tconst), :]

    # Select only the rows which correspond to actors, actresses and directors.
    df = df[in.(df.category, Ref(["actor", "actress", "director"])), :]

    # Grouping by movie, construct the credits dictionary.
    groups = groupby(df, :tconst)
    credits = Dict()
    clusters = Dict()
    for group in groups
        this_tconst = group[1, :tconst]
        this_actors = group[group.category .== "actor", :].nconst
        this_actresses = group[group.category .== "actress", :].nconst
        this_directors = group[group.category .== "director", :].nconst
        this_principals = []

        # Update the clusters dictionary
        for actor in this_actors
            clusters[actor] = 1
        end
        for actress in this_actresses
            clusters[actress] = 2
        end
        for director in this_directors
            clusters[director] = 3
        end

        # Keep at most the first k of each category
        k = 1
        append!(this_principals, this_actresses[1 : min(k, length(this_actresses))])
        append!(this_principals, this_actors[1 : min(k, length(this_actors))])
        append!(this_principals, this_directors[1 : min(k, length(this_directors))])
        credits[this_tconst] = this_principals
    end

    return Imdb(films, actors, clusters, credits)
end

"""
    writeCreditEdgelist(imdbdata, filename)
    writeCreditEdgelist(imdbDirectory)

Store the IMDB credit hyperaph in the edgelist format.

Create five files:
  - filename.edgelist
  - filename.edges
  - filename.vertices
  - filename.gt
  - filename.clusters

If only the imdb directory is specified, then the data is loaded, and then saved, with
filename="credit".

In the filename.edgelist file, there will be a single line for each edge in the hypergraph,
containing a space seperated list of nodes.

The filename.edges file will contain the name of the film corresponding to each edge.

The filename.vertices file will contain the name of the person corresponding to each vertex.
"""
function writeCreditEdgelist(imdbData::Imdb, filename::String)
    # Start by opening the ouput files
    edgelistFile = open("$filename.edgelist", "w")
    edgesFile = open("$filename.edges", "w")
    verticesFile = open("$filename.vertices", "w")
    gtFile = open("$filename.gt", "w")
    clusterFile = open("$filename.clusters", "w")

    # We will construct a dictionary of person IDs -> node IDs as we go along.
    person_node = Dict()

    vᵢ = 1
    for (movie_id, credit_list) in imdbData.credits
        # Add this edge to the edges file
        println(edgesFile, imdbData.films[movie_id])

        for person_id in credit_list
            # Add this person to the person_node dictionary if they are not there already, and
            # save to the vertices file.
            if person_id ∉ keys(person_node)
                person_node[person_id] = vᵢ
                vᵢ += 1

                # Write the name of the vertex
                if person_id ∈ keys(imdbData.actors)
                    println(verticesFile, imdbData.actors[person_id])
                else
                    println(verticesFile, "missing")
                end

                # Write the cluster of the vertex
                if person_id ∈ keys(imdbData.clusters)
                    println(gtFile, imdbData.clusters[person_id])
                else
                    println(verticesFile, "-1")
                end
            end

            print(edgelistFile, "$(person_node[person_id]) ")
        end

        # Add a newline at the end of each line
        print(edgelistFile, "\n")
    end

    # Write the cluster ID file
    print(clusterFile, "actor\nactress\ndirector")

    close(edgelistFile)
    close(edgesFile)
    close(verticesFile)
    close(gtFile)
    close(clusterFile)
end

function writeCreditEdgelist(imdbDirectory::String)
    imdbData = loadimdb(imdbDirectory)
    writeCreditEdgelist(imdbData, joinpath(imdbDirectory, "credit"))
end
