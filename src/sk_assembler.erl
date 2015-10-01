%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module takes a workflow specification, and converts it in into a
%%% set of (concurrent) running processes.
%%%
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_assembler).

-export([
         make/2
        ,run/4
        ]).

-include("skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-spec make(workflow(), pid() | module()) -> pid() .
%% @doc Function to produce a set of processes according to the given workflow 
%% specification.
make(WorkFlow, EndModule) when is_atom(EndModule) ->
  DrainPid = (sk_sink:make(EndModule))(self()),
  make(WorkFlow, DrainPid);
make(WorkFlow, EndPid) when is_pid(EndPid) ->
  MakeFns = [parse(Section) || Section <- WorkFlow],
  lists:foldr(fun(MakeFn, Pid) -> MakeFn(Pid) end, EndPid, MakeFns).

-spec run(pid() | workflow(), input(), boolean(), pid()) -> pid().
%% @doc Function to produce and start a set of processes according to the 
%% given workflow specification and input.
run(WorkFlow, Input, FlowControl_p, WhoToNotify) when is_pid(WorkFlow) ->
  Feeder = sk_source:make(Input, FlowControl_p, WhoToNotify),
  Feeder(WorkFlow);
run(WorkFlow, Input, FlowControl_p, WhoToNotify) when is_list(WorkFlow) ->
  DrainPid = (sk_sink:make())(self()),
  AssembledWF = make(WorkFlow, DrainPid),
  run(AssembledWF, Input, FlowControl_p, WhoToNotify).

-spec parse(wf_item()) -> maker_fun().
%% @doc Determines the course of action to be taken according to the type of 
%% workflow specified. Constructs and starts specific skeleton instances.
parse({bp_seq, InFlight, Fun, InitData}) when is_function(Fun, 2) ->
    sk_bp_seq:make(InFlight, Fun, InitData);
parse({bp_sink, InFlight, Fun, InitData}) when is_function(Fun, 2) ->
    sk_bp_sink:make(InFlight, Fun, InitData);
parse({bp_farm, InFlight, WorkFlow, NWorkers}) ->
    sk_bp_farm:make(InFlight, WorkFlow, NWorkers).
