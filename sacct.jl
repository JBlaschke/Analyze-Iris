#!/usr/bin/env julia

import Pkg
Pkg.activate(joinpath(@__DIR__, "SlurmCLI"), io=devnull)
Pkg.instantiate()


using ArgParse
using SlurmCLI


SETTINGS = ArgParseSettings()
@add_arg_table SETTINGS begin
    "--reservation"
    help = "Parse a reservation"
    arg_type = String
    default = nothing
end
PARSED_ARGS = parse_args(ARGS, SETTINGS)


if ! isnothing(PARSED_ARGS["reservation"])
    lines = open(PARSED_ARGS["reservation"], "r") do f
        readlines(f)
    end

    reservations = SlurmCLI.Reservations.read(lines)
    @show reservations
end
