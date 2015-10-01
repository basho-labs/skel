%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%% 
%%% @doc This module contains the most logic of the most basic kind of skeleton
%%% - `bp_seq'.
%%%
%%% A 'bp_seq' instance is a wrapper for a sequential function, taking an input
%%% from its input stream, applying the function to it and sending the result
%%% on its output stream with full backpressure ultimately controlled by
%%% `bp_sink'.
%%% 
%%% === Example ===
%%% 
%%%   ```skel:run([{bp_seq, fun ?MODULE:p/2},
%%%                {bp_seq, fun ?MODULE:f/2},
%%%                {bp_sink, fun ?MODULE:sink/2}], Input).'''
%%% 
%%%     In this example, Skel is run using two sequential functions. On one 
%%%     process it runs the developer-defined `p/1' on the input `Input', 
%%%     sending all returned results to a second process. On this second 
%%%     process, the similarly developer-defined `f/1' is run on the passed 
%%%     results.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_bp_seq).

-export([
         start/4
        ,make/3
        ]).

-include("skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-define(V(Fmt, Args), io:format(user, Fmt, Args)).
-define(VV(Fmt, Args), io:format(user, "~s ~w ~w: " ++ Fmt, [?MODULE,?LINE,self()]++Args)).

-spec make(non_neg_integer(), worker_fun(), init_data())  -> skel:maker_fun().
%% @doc Spawns a worker process performing the function `WorkerFun'. 
%% Returns an anonymous function that takes the parent process `NextPid'
%% as an argument. 
make(InFlight, WorkerFun, InitData) ->
    fun(NextPid) ->
            spawn_link(?MODULE, start, [NextPid, InFlight, WorkerFun, InitData])
    end.

-spec start(pid(), non_neg_integer(), worker_fun(), init_data()) -> eos.
%% @doc Starts the worker process' task. Recursively receives the worker 
%% function's input, and applies it to said function.
start(NextPid, InFlight, WorkerFun, InitData) ->
    sk_tracer:t(75, self(), {?MODULE, start}, [{next_pid, NextPid}]),
    {ok, FittingState} = WorkerFun({bp_init, InitData}, ignored_placeholder),
    %% ?VV("bp_seq start: inf ~w fs ~w\n", [InFlight, FittingState]),
    receive
        {system, bp_upstream_fitting, UpstreamPid, SourcePid, ChainPids} ->
            %% ?VV("start: my upstream is ~w\n", [UpstreamPid]),
            link(SourcePid),
            NextPid ! {system, bp_upstream_fitting, self(),
                       SourcePid, [self()|ChainPids]},
            sk_utils:bp_signal_upstream(UpstreamPid, InFlight),
            loop(UpstreamPid, NextPid, WorkerFun, FittingState, 0)
    end.

-spec loop(pid(), pid(), function(), term(), integer()) -> eos.
%% @doc Recursively receives and applies the input to the function `DataFun'. 
%% Sends the resulting data message to the process `NextPid'.
loop(UpstreamPid, NextPid, WorkerFun, FittingState, WantCount) ->
    %% ?VV("top: WantCount ~w\n", [WantCount]),
    receive
        {data,_,_} = DataMessage ->
            sk_utils:bp_signal_upstream(UpstreamPid, 1),
            Value = sk_data:value(DataMessage),
            {EmitList, FittingState2} = WorkerFun(Value, FittingState),
            Identifiers = sk_data:identifiers(DataMessage),
            DataMessages = [{data, Emit, Identifiers} || Emit <- EmitList],
            sk_tracer:t(50, self(), NextPid, {?MODULE, data}, [{input, DataMessage}, {output, DataMessages}]),
            WantCount2 = emit_downstream(NextPid, DataMessages, WantCount),
            %% ?VV("top: WantCount2 ~w\n", [WantCount2]),
            loop(UpstreamPid, NextPid, WorkerFun, FittingState2, WantCount2);
        {system, eos} ->
            sk_tracer:t(75, self(), NextPid, {?MODULE, system}, [{message, eos}]),
            %% ?VV("got eos, send to ~w\n", [NextPid]),
            NextPid ! {system, eos},
            eos
    end.

emit_downstream(NextPid, DataMessages, WantCount) ->
    lists:foldl(
      fun(DataMessage, WCount) ->
              WCount2 = sk_utils:bp_get_want_signal(WCount),
              NextPid ! DataMessage,
              WCount2
      end, WantCount, DataMessages).
