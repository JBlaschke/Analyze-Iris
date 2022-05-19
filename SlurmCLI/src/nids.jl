module SlurmNid

isa_nid(x::AbstractString) = (x[1:3] == "nid")

has_range(x::AbstractString) = occursin("[", x)
range_lo(x::AbstractString) = findfirst("[", x)[1]
range_hi(x::AbstractString) = findfirst("]", x)[1]
range_prefix(x::AbstractString) = x[4:range_lo(x)-1]

nid_list_lo(x::AbstractString) = has_range(x) ? range_lo(x) + 1 : 4
nid_list_hi(x::AbstractString) = has_range(x) ? range_hi(x) - 1 : length(x)
nid_list(x::AbstractString) = x[nid_list_lo(x):nid_list_hi(x)]
nid_list_prefix(x::AbstractString) = has_range(x) ? range_prefix(x) : ""

function nids(x::AbstractString)
    nids = Int64[]
    r = split(nid_list(x), ",")
    p = nid_list_prefix(x)
    for sr in r
        s = split(sr, "-")
        if length(s) < 2
            push!(nids, parse(Int64, p*s[1]))
        else
            a, b = s
            append!(nids, collect(parse(Int64, p*a) : parse(Int64, p*b)))
        end
    end
    return nids
end

export nids, isa_nid, has_range

end