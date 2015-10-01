%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains the initialization logic of a Farm skeleton.
%%%
%%% A task farm has the most basic kind of stream parallelism - inputs are
%%% sent to one of `n' replicas of the inner skeleton for processing.
%%%
%%% === Example ===
%%% 
%%% 	```skel:run([{farm, [{seq, fun ?MODULE:p1/1}], 10}], Input)'''
%%% 
%%% 	In this simple example, we produce a farm with ten workers to run the 
%%% sequential, developer-defined function `p/1' using the list of inputs 
%%% `Input'.
%%% 
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_bp_farm).

-export([
         make/3
        ]).

-include("skel.hrl").

-spec make(non_neg_integer(), pos_integer(), workflow()) -> maker_fun().
%% @doc Initialises a Farm skeleton given the number of workers and their 
%% inner-workflows, respectively.
make(InFlight, WorkFlow, NWorkers) ->
  fun(NextPid) ->
    CollectorPid = spawn_link(sk_bp_farm_collector, start, [NWorkers, NextPid]),
    WorkerPids = sk_utils:start_workers(NWorkers, WorkFlow, CollectorPid),
    spawn_link(sk_bp_farm_emitter, start, [InFlight, WorkerPids, CollectorPid])
  end.

