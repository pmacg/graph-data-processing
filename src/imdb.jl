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
    df = DataFrame(CSV.File(filename, select=[:tconst, :nconst]))

    # Select only the rows which correspond to films in our film dictionary.
    df = df[in(keys(films)).(df.tconst), :]

    # Grouping by movie, construct the creditc dictionary.
    groups = groupby(df, :tconst)
    credits = Dict()
    for group in groups
        this_tconst = group[1, :tconst]
        this_principals = Array(group.nconst)
        credits[this_tconst] = this_principals
    end

    return Imdb(films, actors, credits)
end

"""
    writeCreditEdgelist(imdbdata, filename)

Store the IMDB credit hyperaph in the edgelist format.

Create three files:
  - filename.edgelist
  - filename.edges
  - filename.vertices

In the filename.edgelist file, there will be a single line for each edge in the hypergraph,
containing a space seperated list of nodes.

The filename.edges file will contain the name of the film corresponding to each edge.

The filename.vertices file will contain the name of the person corresponding to each vertex.
"""
function writeCreditEdgelist(imdbData, filename)
    # Start by opening the ouput files
    edgelistFile = open("$filename.edgelist", "w")
    edgesFile = open("$filename.edges", "w")
    verticesFile = open("$filename.vertices", "w")

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

                if person_id ∈ keys(imdbData.actors)
                    println(verticesFile, imdbData.actors[person_id])
                else
                    println(verticesFile, "missing")
                end
            end

            print(edgelistFile, "$(person_node[person_id]) ")
        end

        # Add a newline at the end of each line
        print(edgelistFile, "\n")
    end

    close(edgelistFile)
    close(edgesFile)
    close(verticesFile)
end
