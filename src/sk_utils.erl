%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%% @doc This module contains functions designed to start and stop worker 
%%% processes, otherwise known and referred to as simply <em>workers</em>.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_utils).

-export([
         start_workers/3
        ,start_worker_hyb/4
        ,start_workers_hyb/5 
        ,start_worker/2
        ,stop_workers/2
        ,bp_signal_upstream/2
        ,bp_get_want_signal/1
        ,wait_until_dead/1
        ]).

-include("skel.hrl").

-spec start_workers(pos_integer(), workflow(), pid()) -> [pid()].
%% @doc Starts a given number <tt>NWorkers</tt> of workers as children to the specified process <tt>NextPid</tt>. Returns a list of worker Pids.
start_workers(NWorkers, WorkFlow, NextPid) ->
  start_workers(NWorkers, WorkFlow, NextPid, []).

-spec start_workers_hyb(pos_integer(), pos_integer(), workflow(), workflow(), pid()) -> {[pid()],[pid()]}.
start_workers_hyb(NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid) ->
  start_workers_hyb(NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {[],[]}).

-spec start_workers(pos_integer(), workflow(), pid(), [pid()]) -> [pid()].
%% @doc Starts a given number <tt>NWorkers</tt> of workers as children to the 
%% specified process <tt>NextPid</tt>. Returns a list of worker Pids. Inner 
%% function to {@link start_workers/3}, providing storage for partial results.
start_workers(NWorkers,_WorkFlow,_NextPid, WorkerPids) when NWorkers < 1 ->
  WorkerPids;
start_workers(NWorkers, WorkFlow, NextPid, WorkerPids) ->
  NewWorker = start_worker(WorkFlow, NextPid),
  start_workers(NWorkers-1, WorkFlow, NextPid, [NewWorker|WorkerPids]).

start_workers_hyb(NCPUWorkers, NGPUWorkers, _WorkFlowCPU, _WorkFlowGPU, _NextPid, Acc) 
  when (NCPUWorkers < 1) and (NGPUWorkers < 1) ->
    Acc;
start_workers_hyb(NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {CPUWs,GPUWs}) 
  when NCPUWorkers < 1 ->
    NewWorker = start_worker(WorkFlowGPU, NextPid),
    start_workers_hyb(NCPUWorkers, NGPUWorkers-1, WorkFlowCPU, WorkFlowGPU, NextPid, {CPUWs, [NewWorker|GPUWs]});
start_workers_hyb(NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {CPUWs, GPUWs}) ->
    NewWorker = start_worker(WorkFlowCPU, NextPid),
    start_workers_hyb(NCPUWorkers-1, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {[NewWorker|CPUWs],GPUWs}).
    
-spec start_worker(workflow(), pid()) -> pid().
%% @doc Provides a worker with its tasks, the workflow <tt>WorkFlow</tt>. 
%% <tt>NextPid</tt> provides the output process to which the worker's results 
%% are sent.
start_worker(WorkFlow, NextPid) ->
  sk_assembler:make(WorkFlow, NextPid).

-spec start_worker_hyb(workflow(), pid(), pos_integer(), pos_integer()) -> pid().
start_worker_hyb(WorkFlow, NextPid, NCPUWorkers, NGPUWorkers) ->
    sk_assembler:make_hyb(WorkFlow, NextPid, NCPUWorkers, NGPUWorkers).

-spec stop_workers(module(), [pid()]) -> 'eos'.
%% @doc Sends the halt command to each worker in the given list of worker 
%% processes.
stop_workers(_Mod, []) ->
  eos;
stop_workers(Mod, [Worker|Rest]) ->
  sk_tracer:t(85, self(), Worker, {Mod, system}, [{msg, eos}]),
  Worker ! {system, eos},
  stop_workers(Mod, Rest).


-spec bp_signal_upstream(pid(), non_neg_integer()) -> 'ok'.
bp_signal_upstream(UpstreamPid, 0) ->
    ok;
bp_signal_upstream(UpstreamPid, InFlight) ->
    %% io:format(user, "~s ~w ~w: send_want(~w <- ~w)\n", [?MODULE,?LINE,self(),UpstreamPid, InFlight]),
    UpstreamPid ! {system, bp_want, self(), InFlight},
    ok.

-spec bp_get_want_signal(integer()) -> integer().
bp_get_want_signal(0) ->
    WantCount = bp_get_want_messages(0),
    WantCount - 1;
bp_get_want_signal(WantCount) ->
    %% io:format(user, "~w: bp_get_want_signal(~w)\n", [self(), WantCount]),
    WantCount - 1.

bp_get_want_messages(0) ->
    %% Blocking case.
    %% io:format(user, "~s ~w ~w: get_want(0)\n", [?MODULE,?LINE,self()]),
    receive
        {system, bp_want, _Pid, InFlight} ->
            bp_get_want_messages(InFlight)
    end;
bp_get_want_messages(WantCount) ->
    %% io:format(user, "~s ~w ~w: get_want(~w)\n", [?MODULE,?LINE,self(),WantCount]),
    receive
        {system, bp_want, _Pid, InFlight} ->
            bp_get_want_messages(WantCount + InFlight)
    after 0 ->
            WantCount
    end.

-spec wait_until_dead(pid()) -> ok.
wait_until_dead(Pid) ->
    wait_until_dead(Pid, 1).

wait_until_dead(Pid, N) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ok;
        true ->
            timer:sleep(erlang:min(N, 50)),
            wait_until_dead(Pid, N + 1)
    end.
