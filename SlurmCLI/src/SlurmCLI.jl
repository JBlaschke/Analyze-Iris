module SlurmCLI

using JSON, Dates, DataFrames, Parquet, ProgressMeter
using Base: @kwdef, convert, iterate, length


const AdminComment = Dict{String, Any}


export AdminComment


@kwdef struct Reservation
    name::String
    nodelist::String
    starttime::DateTime
    endtime::DateTime
    nodecount::Int64
    features::String
    account::String
end


Base.convert(::Type{DateTime}, s::String) = DateTime(s)


export Reservation


function shell(cmd::Cmd)
    out = Pipe()
    err = Pipe()

    process = run(pipeline(cmd, stdout=out, stderr=err), wait=false)

    stdout = @async String(read(out))
    stderr = @async String(read(err))

    wait(process)
    close(out.in)
    close(err.in)

    return (
        stdout = fetch(stdout),
        stderr = fetch(stderr),
        code = process.exitcode
    )
end


@kwdef struct QueryResult
    result::Any = nothing
    error::String = ""
    code::Int16
end


function sacct_get_jobs(args::Vector{String})
    sacct_cmd = `sacct $(args) -nXPo AdminComment`

    stdout, stderr, code = shell(sacct_cmd)

    if code == 0
        lines = split(stdout, '\n')
        deleteat!(lines, lines .== "")
        return QueryResult(
            result = map(x->JSON.parse(x), lines),
            code   = code
        )
    else
        return QueryResult(
            error = stderr,
            code  = code
        )
    end
end


function sacct_get_jobs(; account=nothing,
                          nodelist=nothing,
                          starttime=nothing,
                          endtime=nothing    )
    args = String[]
    # Always add "-a" to pick up all other user's data
    push!(args, "-a")

    if ! isnothing(nodelist)
        push!(args, "--nodelist=$(nodelist)")
    end
    if ! isnothing(account)
        push!(args, "--account=$(account)")
    end
    if ! isnothing(starttime)
        push!(args, "--starttime=$(starttime)")
    end
    if ! isnothing(endtime)
        push!(args, "--endtime=$(endtime)")
    end

    return sacct_get_jobs(args)
end


export sacct_get_jobs


struct TimePages{T <: Period}
    range::StepRange{DateTime, T}
    start::DateTime
    stop::DateTime
    step::T

    TimePages(
        start::DateTime, stop::DateTime, step::T
    ) where T <: Period = new{T}(
        start:step:stop,
        start,
        stop,
        step
    )
end


function Base.iterate(tp::TimePages, state=nothing)
    next = isnothing(state) ? iterate(tp.range) : iterate(tp.range, state)

    if isnothing(next)
        return nothing
    end

    (item_1, state) = next
    next = iterate(tp.range, state)

    if isnothing(next) && iszero(tp.stop - item_1)
        return nothing
    end

    if isnothing(next) && ! iszero(tp.stop - item_1)
        next = (tp.stop, )
    end
    item_2 = next[1]

    return ((item_1, item_2), state)
end


Base.length(tp::TimePages) = ceil(Int64,(tp.stop - tp.start)/tp.step)


export TimePages


function add_unique!(admin_comments::AdminComment, new_jobs::AdminComment)
    # add unique jobs to the list of admin_comments
    job_ids = map(x->x["jobId"], admin_comments)
    for job in new_jobs
        if ! (job["jobId"] in job_ids)
            push!(admin_comments, job)
            push!(job_ids, job["jobId"])
        end
    end
end


function paginate_collect_jobs(sacct_get_jobs_fn::Function,
                               pages::TimePages;
                               verbose=true, clear_output=true)

    admin_comments = AdminComment[]
    query_status = NamedTuple{
        (:code,      :range,           :err),
        Tuple{Int64, Vector{DateTime}, String}
    }[]

    p  = Progress(
        length(pages); desc="Collected: ", showspeed=true, enabled=verbose
    )

    if clear_output && verbose
        ProgressMeter.ijulia_behavior(:clear)
    end

    update!(p, 0)
    for (ts, te) in pages
        qr = sacct_get_jobs_fn(ts, te)

        next!(p; showvalues=[
                ("current range", (ts, te)),
                ("total collected", length(admin_comments))
            ]
        )

        if qr.code == 0
            push!(query_status, (code=qr.code, range=[ts, te], err=""))
            add_unique!(admin_comments, qr.result)
        else
            push!(query_status, (code=qr.code, range=[ts, te], err=qr.result))
        end
    end

    update!(p, length(pages); showvalues=[
            ("total collected", length(admin_comments))
        ]
    )

    return admin_comments, query_status
end


export paginate_collect_jobs


sacct_collect_jobs(
    res::Reservation, step::Period; verbose=true, clear_output=true
) = paginate_collect_jobs(
    (x, y)->sacct_get_jobs(nodelist=res.nodelist, starttime=x, endtime=y),
    TimePages(res.starttime, res.endtime, step)
)


sacct_collect_jobs(
    account::String, start::DateTime, stop::DateTime, step::Period;
    verbose=true, clear_output=true
) = paginate_collect_jobs(
    (x, y)->sacct_get_jobs(account=account, starttime=x, endtime=y),
    TimePages(start, stop, step)
)


export sacct_collect_jobs


function get_column_descriptors(admin_comments::AdminComment)
    df_columns = Set{Tuple{String, DataType}}()
    for d in admin_comments[1:end]
        for k in keys(d)
            push!(df_columns, (k, typeof(d[k])))
        end
    end

    column_names = map(x->x[1], collect(df_columns))
    column_names, df_columns
end


export get_column_descriptors


isa_list(::Array{T, 1}) where T <: Any = true
isa_list(::Any) = false

export isa_list


function merge_arrays(data::AdminComment)
    d_data = deepcopy(data)
    merged_names = String[]

    for k in keys(d_data)
        if isa_list(d_data[k])
            if length(d_data[k]) > 1
                push!(merged_names, k)
            end

            d_data[k] = join(d_data[k], " ")
        end
    end

    (d_data, merged_names)
end


function to_dataframe!(data::AdminComment, merged::Vector{AdminComment})
    d_data, merged_names = merge_arrays(data)
    df = DataFrame(d_data)
    if length(merged_names) > 0
        merge_d = Dict("original" => data, "merged_names" => merged_names)
        push!(merged, merge_d)
    end
    df
end


export to_dataframe!


function to_dataframe(admin_comments::Vector{AdminComment};
                      verbose=true, clear_output=true     )
    p  = Progress(
        length(admin_comments); dt=1,
        desc="Merging: ", showspeed=true, enabled=verbose
    )

    if clear_output && verbose
        ProgressMeter.ijulia_behavior(:clear)
    end

    merged = AdminComment[]
    df = to_dataframe!(admin_comments[1], merged)
    next!(p)

    for d in admin_comments[2:end]
        df_d = to_dataframe!(d, merged)
        df   = outerjoin(
            df, df_d,
            on = intersect(names(df), names(df_d)),
            matchmissing = :equal
        )

        next!(p)
    end

    (df, merged)
end


export to_dataframe


end # module
