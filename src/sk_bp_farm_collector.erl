%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains the collector logic of a Farm skeleton.
%%%
%%% A task farm has the most basic kind of stream parallelism - inputs are
%%% sent to one of `n' replicas of the inner skeleton for processing.
%%%
%%% The collector takes inputs off the inner-skeletons' output streams, sending
%%% them out on the farm skeleton's output stream. It does not preserve 
%%% ordering.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_bp_farm_collector).

-export([
         start/2
        ]).

-include("skel.hrl").

-spec start(pos_integer(), pid()) -> 'eos'.
%% @doc Initialises the collector; forwards any and all output from the inner-
%% workflow to the sink process at `NextPid'.
start(NWorkers, NextPid) ->
    sk_tracer:t(75, self(), {?MODULE, start}, [{num_workers, NWorkers}, {next_pid, NextPid}]),
    Workers = get_worker_pids(NWorkers),
    handle_last_upstream_fitting_msg(Workers, NextPid),
    receive
        {system, bp_want, NextPid, _N} ->
            %% This is the initial InFlight value from downstream.
            %% We convert it into 1 and send to all workers.
            [sk_utils:bp_signal_upstream(Pid, 1) || Pid <- Workers],
            loop(NWorkers, NextPid)
    end.

-spec loop(pos_integer(), pid()) -> 'eos'.
%% @doc Worker-function for {@link start/2}. Recursively receives, and 
%% forwards, any output messages from the inner-workflow. Halts when the `eos' 
%% system message is received, and only one active worker process remains.
loop(NWorkers, NextPid) ->
    receive
        {data, WorkerPid, _, _} = DataMessage ->
            sk_utils:bp_signal_upstream(WorkerPid, 1),
            sk_tracer:t(50, self(), NextPid, {?MODULE, data}, [{input, DataMessage}]),
            NextPid ! DataMessage,
            loop(NWorkers, NextPid);
        {system, eos} when NWorkers =< 1 ->
            sk_tracer:t(75, self(), NextPid, {?MODULE, system}, [{msg, eos}, {remaining, 0}]),
            NextPid ! {system, eos},
            eos;
        {system, eos} ->
            sk_tracer:t(85, self(), {?MODULE, system}, [{msg, eos}, {remaining, NWorkers-1}]),
            loop(NWorkers-1, NextPid)
    end.

get_worker_pids(0) ->
    [];
get_worker_pids(N) ->
    Me = self(),
    receive
        {system, bp_upstream_fitting, WorkerPid, SourcePid, FarmPids}=Msg
          when SourcePid == Me ->
            ?VV("start: my worker upstream is ~w\n", [WorkerPid]),
            link(WorkerPid),
            FarmPids ++ get_worker_pids(N - 1)
    end.

handle_last_upstream_fitting_msg(Workers, NextPid) ->
    receive
        {system, bp_upstream_fitting, EmitterPid, SourcePid, ChainPids} ->
            ?VV("start: my emitter upstream is ~w\n", [EmitterPid]),
            false = lists:member(farm, ChainPids), % sanity check
            link(EmitterPid),
            NextPid ! {system, bp_upstream_fitting, self(), SourcePid,
                       [self()|Workers] ++ ChainPids}
    end.
    
