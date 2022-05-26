module Reservations

using Dates
using Base: @kwdef, convert, parse, parse


@kwdef struct ReservationDescriptor
    name::String
    nodelist::String
    starttime::DateTime
    endtime::DateTime
    nodecount::Int64
    features::String
    account::String
end


struct Duration
    value::DateTime
end


@kwdef struct SlurmReservation
    ReservationName::String
    Nodes::String
    StartTime::DateTime
    EndTime::DateTime
    Duration::Duration
    NodeCnt::Int64
    CoreCnt::Int64
    Features::String
    PartitionName::String
    Flags::String
    TRES::String
    Users::String
    Groups::String
    Accounts::String
    Licenses::String
    State::String
    BurstBuffer::String
    Watts::String
    MaxStartDelay::Duration
end


Base.convert(::Type{DateTime}, s::AbstractString) = DateTime(s)

Base.convert(::Type{Int64}, s::AbstractString) = parse(Int64, s)

function Base.convert(::Type{Duration}, s::AbstractString)
    if s .== "(null)"
        s = "00:00:00"
    end

    if contains(s, "-")
        return Duration(DateTime(s, "d-HH:MM:SS"))
    else
        return Duration(DateTime(s, "HH:MM:SS"))
    end
end


function ReservationDescriptor(r::SlurmReservation)
    ReservationDescriptor(;
        name=r.ReservationName,
        nodelist=r.Nodes,
        starttime=r.StartTime,
        endtime=r.EndTime,
        nodecount=r.NodeCnt,
        features=r.Features,
        account=r.Accounts
    )
end

export ReservationDescriptor, SlurmReservation, Duration


function Base.parse(::Type{SlurmReservation}, s::AbstractString)
    r_dict = Dict{Symbol, Any}()
    for r_bit in strip.(split(s, ' '))
        if length(r_bit) == 0
            continue
        end
        k, v = split(r_bit, "=")
        r_dict[Symbol(k)] = v
    end
    SlurmReservation(;r_dict...)
end


function read(lines::Vector{T}) where T <: AbstractString
    delim = findall((lines .== "") .> 0)
    # EOF is presumed the end of reservation descriptor, if not empty line
    if length(delim) == 0
        delim = [length(lines)]
    end

    reservation_str = String[]
    delim_lo        = 1
    for delim_hi in delim
        push!(reservation_str, strip(join(lines[delim_lo:delim_hi], " ")))
        delim_lo = delim_hi
    end

    reservations = SlurmReservation[]
    for r in reservation_str
        push!(reservations, parse(SlurmReservation, r))
    end

    reservations
end


export read


end