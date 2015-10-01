%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains the emitter logic of a Farm skeleton.
%%%
%%% A task farm has the most basic kind of stream parallelism - inputs are
%%% sent to one of `n' replicas of the inner skeleton for processing.
%%%
%%% The emitter takes inputs off the skeleton's input stream and assigns each
%%% one to one of the input streams of the 'n' inner skeletons.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_bp_farm_emitter).

-export([
         start/3
        ]).

-include("skel.hrl").

-spec start(non_neg_integer(), [pid(),...], pid()) -> 'eos'.
%% @doc Initialises the emitter. Sends input as messages to the list of 
%% processes given by `Workers'.
%%
%% Note: Each of our workers will send an initial bp_want,1 message to us,
%%       and we will forward them to our upstream.  Therefore, the total
%%       number of inflight will be our inflight + # of workers.
start(InFlight, Workers, CollectorPid) ->
    sk_tracer:t(75, self(), {?MODULE, start}, [{workers, Workers}]),
    receive
        {system, bp_upstream_fitting, UpstreamPid, SourcePid, ChainPids} ->
            %% ?VV("start: my upstream is ~w\n", [UpstreamPid]),
            link(SourcePid),
            %% Use sourcePid=CollectorPid to signal to farm_collector
            [Pid ! {system, bp_upstream_fitting, self(), CollectorPid, []} ||
                Pid <- Workers],
            %% Send the "real" bp_upstream_fitting message directly to
            %% collector.
            CollectorPid ! {system, bp_upstream_fitting, self(), SourcePid,
                            [self()|ChainPids]},
            sk_utils:bp_signal_upstream(UpstreamPid, InFlight),
            loop(UpstreamPid, Workers, CollectorPid)
    end.

loop(UpstreamPid, Workers, CollectorPid) ->
    receive
        {system, bp_want, WorkerPid, N} when N > 1 ->
            %% If this ever happens, "convert" to N=1 messages to make the
            %% rest of our code simpler.
            [self() ! {system, bp_want, WorkerPid, 1} || _ <- lists:seq(1,N)],
            loop(UpstreamPid, Workers, CollectorPid);
        {data, _, _, _} = DataMessage ->
            sk_utils:bp_signal_upstream(UpstreamPid, 1),
            receive
                {system, bp_want, WorkerPid, 1} ->
                    WorkerPid ! sk_data:set_sender(DataMessage),
                    loop(UpstreamPid, Workers, CollectorPid)
            end;
        {system, eos} ->
            ?VV("eos: workers ~w\n", [Workers]),
            sk_utils:stop_workers(?MODULE, Workers),
            %% [sk_utils:wait_until_dead(Pid) || Pid <- Workers],
            %% ?VV("eos: collector ~w\n", [CollectorPid]),
            %% CollectorPid ! eos,
            eos
    end.
