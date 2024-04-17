using Distributed

addprocs(2)

@everywhere begin
    import Pkg
    Pkg.activate(joinpath(@__DIR__, "SlurmCLI"))
    Pkg.instantiate()
end

@everywhere using SlurmCLI


@everywhere begin
    using Base: @kwdef
    using CodecZlib
    using DelimitedFiles
    using Chain
    using JSON
    using Dates
end

@everywhere @kwdef struct Job
    id::Int64                    = 0
    admin_comment::Vector{UInt8} = UInt8[]
    valid::Bool                  = false
end


fn_ret_type(fn, in_type::DataType) = Base.return_types(fn, (in_type,))[1]

function launch_monitor(processor; buffer_size=32)
    function remote_monitor(fn, jobs, results)
        @sync while true
            job = try
                take!(jobs)
            catch y
                break
            end
            t = @async fn(job)
            @async put!(results, fetch(t))
        end
    end

    jobs    = RemoteChannel(()->Channel{Job}(buffer_size))
    results = RemoteChannel(
            ()->Channel{fn_ret_type(processor, Job)}(buffer_size)
        )

    for p in workers()
        remote_do(remote_monitor, p, processor, jobs, results)
    end

    return jobs, results
end


function launch_consumer(processor, jobs; buffer_size=32)
    function remote_monitor(fn, jobs, results)
        @sync while true
            job = try
                take!(jobs)
            catch y
                break
            end
            t = @async fn(job)
            @async put!(results, fetch(t))
        end
    end

    # jobs    = RemoteChannel(()->Channel{Job}(buffer_size))
    results = RemoteChannel(
            ()->Channel{fn_ret_type(processor, Job)}(buffer_size)
        )

    for p in workers()
        remote_do(remote_monitor, p, processor, jobs, results)
    end

    return results
end


function collect!(
        results::A; collect_time=1,
    ) where {
             T <: Job,
             S <: AbstractChannel{T},
             A <: Union{S, RemoteChannel{S}}
            }

    collected = Vector{Job}()

    t = @async while true
        fd = take!(results)
        push!(collected, fd)
    end

    sleep(collect_time)
    schedule(t, InterruptException(), error=true)

    return collected
end


jobs, admin_comments = launch_monitor(
    x->begin
        sacct = SlurmCLI.shell(`sacct -nPXo AdminComment -j $(x.id)`)
        valid = length(sacct.stdout)>0 && length(sacct.stderr)==0
        sacct_compressed = transcode(ZlibCompressor, sacct.stdout)
        Job(id=x.id, admin_comment=sacct_compressed, valid=valid)
    end;
    buffer_size=1024
)


@everywhere begin
    path = joinpath(
        ENV["CFS"], "nstaff", "blaschke", "slurm_3", "cori_$(myid()).stream"
    )
    stream = ZlibCompressorStream(open(path, "a"))
end

results = launch_consumer(
    x->begin
        if ! x.valid
            return x
        end

        decompressed_comment = @chain x.admin_comment begin
            transcode(ZlibDecompressor, _)
            String
        end
        # path = joinpath(
        #     ENV["CFS"], "nstaff", "blaschke", "slurm_3", "cori_$(myid()).stream"
        # )
        # open(ZlibCompressorStream, path, "a") do stream
            size::Int64 = length(decompressed_comment)
            write(stream, size)
            write(stream, decompressed_comment)
        # end
        
        return x
    end,
    admin_comments;
    buffer_size=1024
)

N=100000000
@async for i=1:N
    put!(jobs, Job(id=i))
end

using ProgressMeter

p  = Progress(
    N; desc="Collected: ", showspeed=true, enabled=true
)
update!(p, 0)

global total_collected = 0
while true
    admn = collect!(admin_comments)
    global total_collected += length(admn)
    update!(p, total_collected)

    if length(admn) == 0
        sleep(1)
        continue
    end
    if total_collected>=N && (length(admn) == 0)
        break
    end
end


@everywhere close(stream)
