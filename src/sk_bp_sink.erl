%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%% 
%%% @doc This module contains the sink logic.
%%%
%%% A sink is a process that accepts inputs off the final output stream in a 
%%% skeleton workflow. 
%%%
%%% Two kinds of sink are provided - a list accumulator sink (the default) and
%%% a module sink, that uses a callback module to deal with the data.
%%%
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_bp_sink).

-export([
         make/3
        ,start/4
        ]).

-include("skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-define(V(Fmt, Args), io:format(user, Fmt, Args)).
-define(VV(Fmt, Args), io:format(user, "~s ~w ~w: " ++ Fmt, [?MODULE,?LINE,self()]++Args)).

%% -callback init() ->
%%     {ok, State :: term()} |
%%     {stop, State :: term()}.

%% -callback next_input(NextInput :: term(), State :: term()) ->
%%     {ok, NewState :: term()} |
%%     {stop, NewState :: term()}.

%% -callback terminate(State :: term()) ->
%%     term().

-spec make(non_neg_integer(), function(), term()) -> maker_fun().
%% @doc Creates the process to which the final results are sent.
make(InFlight, WorkerFun, InitData) ->
  fun(Pid) ->
    spawn_link(?MODULE, start, [Pid, InFlight, WorkerFun, InitData])
  end.

-spec start(pid(), non_neg_integer(), function(), term()) -> 'eos'.
%% @doc Sets the sink process to receive messages from other processes.
start(NextPid, InFlight, WorkerFun, InitData) ->
    {ok, FittingState} = WorkerFun({bp_init, InitData}, ignored_placeholder),
    %% ?VV("sink: inf ~w fs ~w\n", [InFlight, FittingState]),
    receive
        {system, bp_upstream_fitting, UpstreamPid, SourcePid, ChainPids} ->
            %% ?VV("sink: send to source: ~p\n", [lists:reverse(ChainPids)]),
            [link(Pid) || Pid <- [SourcePid|ChainPids]],
            AllChainPids = lists:reverse([self()|ChainPids]),
            SourcePid ! {system, bp_chain_pids, AllChainPids},
            %% ?VV("start: my upstream is ~w\n", [UpstreamPid]),
            sk_utils:bp_signal_upstream(UpstreamPid, InFlight),
            loop_acc(UpstreamPid, NextPid, WorkerFun, FittingState)
    end.

-spec loop_acc(pid(), pid(), function(), term()) -> 'eos'.
%% @doc Recursively recieves messages, collecting each result in a list. 
%% Returns the list of results when the system message <tt>eos</tt> is 
%% received. 
loop_acc(UpstreamPid, NextPid, WorkerFun, FittingState) ->
    %% ?VV("loop_acc top\n", []),
    receive
        {data, _, _} = DataMessage ->
            sk_utils:bp_signal_upstream(UpstreamPid, 1),
            Value = sk_data:value(DataMessage),
            sk_tracer:t(50, self(), {?MODULE, data}, [{input, DataMessage}, {value, Value}]),

            {ok, FittingState2} = WorkerFun(Value, FittingState),
            loop_acc(UpstreamPid, NextPid, WorkerFun, FittingState2);
        {system, eos} ->
            sk_tracer:t(75, self(), {?MODULE, system}, [{msg, eos}]),
            %% ?VV("loop_acc received eos\n", []),
            {ok, _FittingState2} = WorkerFun(bp_eoi, FittingState),
            eos
    end.
