%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%% @doc This module contains the source logic.
%%%
%%% A source is a process that provides the given inputs to the first process
%%% in a skeleton workflow.
%%%
%%% Two kinds of sources are provided - a list source (the default) and
%%% a module source, that uses a callback module to deal with the data.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_source).

-export([
         make/2
        ,start/3
        ]).

-include("skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-define(V(Fmt, Args), io:format(user, Fmt, Args)).
-define(VV(Fmt, Args), io:format(user, "~s ~w ~w: " ++ Fmt, [?MODULE,?LINE,self()]++Args)).

-callback init() ->
    {ok, State :: term()} |
    {no_inputs, State :: term()}.

-callback next_input(State :: term()) ->
    {input, NextInput :: term(), NewState :: term()} |
    {ignore, NewState :: term()} |
    {eos, NewState :: term()}.

-callback terminate(State :: term()) ->
    ok.

%% @doc Creates a new child process using Input, given the parent process 
%% <tt>Pid</tt>.
-spec make(input(), boolean()) -> maker_fun().
make(Input, FlowControl_p) ->
  fun(Pid) ->
    spawn(?MODULE, start, [Input, Pid, FlowControl_p])
  end.

%% @doc Transmits each input in <tt>Input</tt> to the process <tt>NextPid</tt>.
%% @todo add documentation for the callback loop
-spec start(input(), pid(), boolean()) -> 'eos'.
start(Input, NextPid, FlowControl_p) when is_list(Input) ->
    NextPid ! {system, bp_upstream_fitting, self()},
    %% ?VV("start: tell ~w I am its upstream\n", [NextPid]),
    list_loop(Input, NextPid, FlowControl_p, 0);
start(InputMod, NextPid, _FlowControl_p) when is_atom(InputMod) ->
  case InputMod:init() of
    {ok, State} -> callback_loop(InputMod, State, NextPid);
    {no_inputs, State}  ->
      send_eos(NextPid),
      InputMod:terminate(State)
  end.

%% @doc Recursively sends each input in a given list to the process 
%% <tt>NextPid</tt>.
list_loop([], NextPid, _FlowControl_p, _WantCount) ->
    send_eos(NextPid);
list_loop([Input|Inputs], NextPid, FlowControl_p, WantCount) ->
    WantCount2 = if FlowControl_p ->
                         sk_utils:bp_get_want_signal(WantCount);
                    true ->
                         WantCount
                 end,
    send_input(Input, NextPid),
    list_loop(Inputs, NextPid, FlowControl_p, WantCount2).

%% @todo doc
callback_loop(InputMod, State, NextPid) ->
  case InputMod:next_input(State) of
    {input, NextInput, NewState} ->
      send_input(NextInput, NextPid),
      callback_loop(InputMod, NewState, NextPid);
    {ignore, NewState} ->
      callback_loop(InputMod, NewState, NextPid);
    {eos, NewState} ->
      send_eos(NextPid),
      InputMod:terminate(NewState),
      eos
  end.

%% @doc <tt>Input</tt> is formatted as a data message and sent to the 
%% process <tt>NextPid</tt>. 
send_input(Input, NextPid) ->
  DataMessage = sk_data:pure(Input),
  sk_tracer:t(50, self(), NextPid, {?MODULE, data}, [{output, DataMessage}]),
  NextPid ! DataMessage.

send_eos(NextPid) ->
  sk_tracer:t(75, self(), NextPid, {?MODULE, system}, [{msg, eos}]),
  NextPid ! {system, eos},
  eos.


