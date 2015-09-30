%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%% @doc This module is the root module of the 'Skel' library, including 
%%% entry-point functions.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(skel).

-export([
         run/4
        ,bp_do/2
        ]).

-include("skel.hrl").

%% @doc Primary entry-point function to the Skel library. Runs a specified 
%% workflow passing <tt>Input</tt> as input. Does not receive or return any
%% output from the workflow.
%% 
%% <h5>Example:</h5>
%%    ```skel:run([{seq, fun ?MODULE:p/1}], Images)'''
%%
%%    Here, skel runs the function <tt>p</tt> on all items in the 
%%    list <tt>Images</tt> using the Sequential Function wrapper.
%%

-spec run(workflow(), input(), boolean(), pid()) -> pid().
run(WorkFlow, Input, FlowControl_p, PidToNotify) ->
  sk_assembler:run(WorkFlow, Input, FlowControl_p, PidToNotify).

-spec bp_do(workflow(), list()) -> list().
bp_do(WorkFlow, Input) ->
    FeederPid = run(WorkFlow, Input, true, self()),
    receive
        {chain_pids, _Pid, ChainPids}=_Msg ->
            {FeederPid, ChainPids}
    after 1000 ->
            exit(skel_setup_timeout)
    end.
