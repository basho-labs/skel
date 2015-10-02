%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(sk_basic_eqc).

-ifdef(TEST).
-ifdef(EQC).

-compile(export_all).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(QC_FMT(Fmt, Args),
        io:format(user, Fmt, Args)).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> ?QC_FMT(Str, Args) end, P)).

-spec test() -> any().
-spec basic_prop_test_() -> tuple().

gen_workflow() ->
    list(frequency([{10, gen_seq()},
                    { 1, gen_farm()}])).

gen_seq() ->
    frequency([{9, gen_seq_prepend()},
               {1, gen_seq_duplicate()}]).

gen_seq_prepend() ->
    {bp_seq, 2, fun prepend/3, x}.

gen_seq_duplicate() ->
    {bp_seq, 2, fun duplicate/3, 3}.

%% We can easily create a workflow with too many processes for a
%% default Erlang VM when using a simple 'rebar eunit' command.  Not
%% too many workers, and the two constants for 'div' operations below are 

gen_n_workers() ->
    choose(1, 2).

gen_farm() ->
    ?SIZED(Size, gen_farm_workflow(Size div 4)).

gen_farm_workflow(0) ->
    {bp_farm, 2, gen_workflow(), gen_n_workers()};
gen_farm_workflow(X) ->
    gen_farm_workflow(X div 11).
    
gen_inputs() ->
    ?LET(X , nat(),
    ?LET(X2, oneof([X, X*X]),
         lists:duplicate(X2, []))).

basic_prop() ->
    ?FORALL({WorkFlow0, Inputs}, {gen_workflow(), gen_inputs()},
    ?IMPLIES(begin
                 Flat = flatten(WorkFlow0),
                 NumDuplicateOps = length([x || duplicate <- Flat]),
                 ExpectOutputs = length(Inputs) * math:pow(3, NumDuplicateOps),
                 %% Really, let's not spend lots of time on a single test case
                 ExpectOutputs < 20*1000
             end,
            begin
                MyRef = make_ref(),
                Sink = {bp_sink, 2, fun sink/3, {self(),MyRef}},
                WorkFlow = WorkFlow0 ++ [Sink],
                {Source, Workers} = skel:bp_do(WorkFlow, Inputs),
                Res = receive
                          {sink_final_result, MyRef, Val} ->
                              Val
                      end,
                %% io:format(user, ">", []),
                DeadFun = fun(Pid) -> not is_process_alive(Pid) end,

                %% All items received by the sink should be identical.
                SinkGotZeroOrOne = case lists:usort(Res) of
                                       []  -> true;
                                       [_] -> true;
                                       _   -> false
                                   end,
                %% Number of outputs should be length of inputs
                %% multiplied by (the number of duplicate ops + 1).
                Flat = flatten(WorkFlow0),
                NumPrependOps = length([x || prepend <- Flat]),
                NumDuplicateOps = length([x || duplicate <- Flat]),
                NumFarmOps = length([x || farm <- Flat]),
                NumInputs = length(Inputs),
                ExpectOutputs = NumInputs * math:pow(3, NumDuplicateOps),
                ListLenGood = if Res == [] ->
                                      true;
                                 length(hd(Res)) > 7, length(NumDuplicateOps) > 0 ->
                                      false;
                                 true ->
                                      length(hd(Res)) == NumPrependOps
                              end,

                timer:sleep(1),                 % give death a chance
                AllDead = lists:all(DeadFun, [Source|Workers]),
                ?WHENFAIL(
                ?QC_FMT("Flat ~w\n# prepends ~w\n# dups ~w\n# inputs ~w\n# outputs ~w\n", [Flat, NumPrependOps, NumDuplicateOps, NumInputs, length(Res)]),
                measure(num_inputs, length(Inputs),
                measure(num_outputs, length(Res),
                measure(num_prepend, NumPrependOps,
                measure(num_duplicate, NumPrependOps,
                measure(num_farm, NumFarmOps,
                conjunction([
                             {uniques, SinkGotZeroOrOne},
                             {individual_list_length, ListLenGood},
                             {expect_outputs, length(Res) == ExpectOutputs},
                             {all_dead, AllDead}
                            ])))))))
            end)).

sink(bp_init, {_,_}=MyParent, _Ignore) ->
    {ok, {[], MyParent}};
sink(bp_eoi, _, {Acc, {MyParentPid,Ref}}=State) ->
    MyParentPid ! {sink_final_result, Ref, Acc},
    {ok, State};
sink(bp_work, Data, {Acc, MyParent}) ->
    {ok, {[Data|Acc], MyParent}}.

prepend(bp_init, Data, _Ignore) ->
    {ok, Data};
prepend(bp_eoi, _, S) ->
    {ok, S};
prepend(bp_work, List, S) ->
    Res = [S|List],
    {done, [Res], S}.

duplicate(bp_init, Max, _Ignore) ->
    {ok, Max};
duplicate(bp_eoi, _, Max) ->
    {ok, Max};
duplicate(bp_work, X, Max) ->
    Res = X,
    {continue, [Res], {2,X}, Max};
duplicate(bp_continue, {NextInt, X}, Max) ->
    Emit = X,
    if NextInt == Max ->
            {done, [Emit], Max};
       true ->
            {continue, [Emit], {NextInt+1, X}, Max}
    end.

basic_prop_test_() ->
    Time = case os:getenv("EQC_TIME") of
               false -> 2;
               T     -> list_to_integer(T)
           end,
    {timeout, Time+120,
     fun() -> true = eqc:quickcheck(
                       eqc:testing_time(Time,
                                        ?QC_OUT(basic_prop())))
     end}.

%%%%%

flatten(WorkFlow) when is_list(WorkFlow) ->
    lists:flatten([flatten(X) || X <- WorkFlow]);
flatten({bp_seq, _, _, _}=Tuple) ->
    %% !@#$! can't use fun func/arity in a guard.....
    Prepend = gen_seq_prepend(),
    Duplicate = gen_seq_duplicate(),
    if Tuple == Prepend ->
            prepend;
       Tuple == Duplicate ->
            duplicate
    end;
flatten({bp_farm,_,WorkFlow,_}) ->
    [farm|flatten(WorkFlow)].


-endif. % EQC
-endif. % TEST
