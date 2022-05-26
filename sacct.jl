#!/usr/bin/env julia

import Pkg
Pkg.activate(joinpath(@__DIR__, "SlurmCLI"), io=devnull)
Pkg.instantiate()


using ArgParse, JSON, Dates, Parquet
using SlurmCLI


SETTINGS = ArgParseSettings()
@add_arg_table SETTINGS begin
    "--reservation_file"
    help = "Collect all jobs from a reservation [path to `scontrol show reservation` output]"
    arg_type = String
    default = nothing
    "--account"
    help = "Collect all jobs from an account [name of account], must include start and end times"
    arg_type = String
    default = nothing
    "--starttime"
    help = "Start time to look for jobs"
    arg_type = String
    default = nothing
    "--endtime"
    help = "End time time to look for jobs"
    arg_type = String
    default = nothing
    "--nodelist"
    help = "List of Node IDs"
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
println("Using formats: $(join(FORMATS, ", "))")

if ! isnothing(PARSED_ARGS["reservation_file"])
    lines = open(PARSED_ARGS["reservation_file"], "r") do f
        readlines(f)
    end

    reservations = SlurmCLI.Reservations.read(lines)
    for reservation in reservations
        rd = SlurmCLI.Reservations.ReservationDescriptor(reservation)
        println(" + Collecting: $(rd.name)")

        sav, status = sacct_collect_jobs(rd, Day(1))
        for e in filter(x->x.code>0, status)
            println(" ! Error occurred for : $(e.range[1]),$(e.range[2]))")
            println("   +------>    status : $(e.status)")
            println("   `------>   message : $(e.err)")
        end

        if any(FORMATS .== "json")
            open(joinpath(DEST, "$(rd.name)_admincomment.json"), "w") do f
                JSON.print(f, sav, 4)
            end
        end

        if any(FORMATS .== "parquet")
            df, merged = to_dataframe(sav)
            open(joinpath(DEST, "$(rd.name)_merged.json"), "w") do f
                JSON.print(f, merged, 4)
            end

            write_parquet(joinpath(DEST, "$(rd.name).parquet"), df)
        end
    end
end

if ! isnothing(PARSED_ARGS["account"])

    account = PARSED_ARGS["account"]
    start   = DateTime(PARSED_ARGS["starttime"])
    stop    = DateTime(PARSED_ARGS["endtime"])
    sav, status = sacct_collect_jobs(account, start, stop, Day(1))
    for e in filter(x->x.code>0, status)
        println(" ! Error occurred for : $(e.range[1]),$(e.range[2]))")
        println("   +------>    status : $(e.status)")
        println("   `------>   message : $(e.err)")
    end

    if any(FORMATS .== "json")
        open(joinpath(DEST, "$(account)_admincomment.json"), "w") do f
            JSON.print(f, sav, 4)
        end
    end

    if any(FORMATS .== "parquet") && (length(sav) > 0)
        df, merged = to_dataframe(sav)
        open(joinpath(DEST, "$(account)_merged.json"), "w") do f
            JSON.print(f, merged, 4)
        end

        write_parquet(joinpath(DEST, "$(account).parquet"), df)
    end
end
