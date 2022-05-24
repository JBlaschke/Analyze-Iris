#!/usr/bin/env julia

import Pkg
Pkg.activate(joinpath(@__DIR__, "SlurmCLI"), io=devnull)
Pkg.instantiate()


using ArgParse, JSON, Dates
using SlurmCLI


SETTINGS = ArgParseSettings()
@add_arg_table SETTINGS begin
    "--reservation"
    help = "Parse a reservation [path to `scontrol show reservation` output]"
    arg_type = String
    default = nothing
    "--output_format"
    help = "Output formats [comma seperated list: json, parquet]"
    arg_type = String
    default = "json,parquet"
    "--output_dest"
    help = "Output destination [path to new folder]"
    arg_type = String
    required = true
end
PARSED_ARGS = parse_args(ARGS, SETTINGS)

FORMATS = split(PARSED_ARGS["output_format"], ',')

DEST = PARSED_ARGS["output_dest"]
if isdir(DEST)
    error("Destination $(DEST) is not free.")
    exit()
end

mkdir(DEST)

println("Writing SACCT data to $(DEST)")
println("Using formats: $(FORMATS)")

if ! isnothing(PARSED_ARGS["reservation"])
    lines = open(PARSED_ARGS["reservation"], "r") do f
        readlines(f)
    end

    reservations = SlurmCLI.Reservations.read(lines)
    for reservation in reservations
        rd = SlurmCLI.Reservations.ReservationDescriptor(reservation)
        println(" + Collecting: $(rd)")

        sav, status = sacct_collect_jobs(rd, Day(1))
        for e in filter(x->x.code>0, status)
            println(" ! Error occurred for : $(e.range[1]),$(e.range[2]))")
            println("   +------>    status : $(e.status)")
            println("   `------>   message : $(e.err)")
        end

        if contains(FORMATS, "json")
            open(joinpath(DEST, "$(rd.name)_admincomment.json"), "w") do f
                JSON.print(f, sav, 4)
            end
        end
    end
end
