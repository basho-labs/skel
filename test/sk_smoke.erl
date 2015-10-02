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

-module(sk_smoke).
-compile(export_all).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(V(Fmt, Args), io:format(user, Fmt, Args)).
-define(VV(Fmt, Args), io:format(user, "~s ~w ~w: " ++ Fmt, [?MODULE,?LINE,self()]++Args)).

-spec test() -> term().

-spec smoke_bp_sink_test() -> term().
smoke_bp_sink_test() ->
    Inputs = lists:seq(1,20),
    Me = self(),
    MyRef = make_ref(),

    %% timer:sleep(50),?VV("\n", []),?VV("smoke_bp_sink_test: top\n", []),
    {_FeederPid, _WorkPids} =
        skel:bp_do([{bp_sink, 2, fun bp_demo_sink/3, {Me, MyRef}}], Inputs),
    Res1 = receive
               {sink_final_result, MyRef, Val} ->
                   Val
           end,
    Inputs = Res1.

-spec smoke_bp_seq_test() -> term().
smoke_bp_seq_test() ->
    Inputs = lists:seq(1,20),
    Me = self(),
    MyRef = make_ref(),

    %% timer:sleep(50),?VV("\n", []),?VV("smoke_bp_seq_test: top\n", []),
    {_FeederPid, _WorkPids} =
         skel:bp_do([{bp_seq,  2, fun bp_demo_identity/3, init_data_ignored},
                     {bp_seq,  2, fun bp_double/3, 22},
                     {bp_seq,  2, fun bp_half/3, 77.4},
                     {bp_seq,  2, fun bp_truncate/3, -2.22},
                     {bp_sink, 2, fun bp_demo_sink/3, {Me,MyRef}}], Inputs),
    %% ?VV("Feeder ~p WorkPids ~p\n", [_FeederPid, _WorkPids]),
    Res2 = receive
               {sink_final_result, MyRef, Val} ->
                   Val
           end,
    Inputs = Res2.

%% Remove the "_SKIP" suffix from the name to run a verbose test that
%% shows the pipe-style parallelism with the bp_seq list with a
%% bp_sink at the end.

-spec smoke_bp_seq_sleep_test_SKIP() -> term().
smoke_bp_seq_sleep_test_SKIP() ->
    Inputs = lists:seq(1,10),
    Me = self(),
    MyRef = make_ref(),

    {_FeederPid, _WorkPids} =
         skel:bp_do([{bp_seq,  2, fun bp_verbose_sleep/3, 50},
                     {bp_seq,  2, fun bp_verbose_sleep/3, 70},
                     {bp_seq,  2, fun bp_verbose_sleep/3, 90},
                     {bp_seq,  2, fun bp_verbose_sleep/3, 0},
                     {bp_sink, 2, fun bp_demo_sink/3, {Me,MyRef}}], Inputs),
    Res2 = receive
               {sink_final_result, MyRef, Val} ->
                   Val
           end,
    %% The last sleep with a 0 sleep time doesn't emit anything, so
    %% the sink doesn't collect anything.  But, nevertheless, we run
    %% to completion, yay!
    [] = Res2.

-spec smoke_bp_crash1_test() -> term().
smoke_bp_crash1_test() ->
    CrashAfter = 5,
    ExitReason = goodie_we_get_to_crash,
    Inputs = lists:seq(1, CrashAfter+2),  % We'll crash if length >= CrashAfter
    Me = self(),
    MyRef = make_ref(),
    process_flag(trap_exit, true),

    try
        {FeederPid, WorkPids} =
            skel:bp_do([{bp_seq, 2, fun bp_demo_identity/3, init_data_ignored},
                        {bp_seq, 2, fun bp_demo_identity/3, init_data_ignored},
                        {bp_seq, 2, fun bp_crash_after/3, {CrashAfter,ExitReason}},
                        {bp_seq, 2, fun bp_demo_identity/3, init_data_ignored},
                        {bp_sink,2, fun bp_demo_sink/3, {Me,MyRef}}], Inputs),
        Pids = [FeederPid|WorkPids],
        [ok = sk_utils:wait_until_dead(P) || P <- Pids],
        Statuses = [begin
                        Status = receive {'EXIT', P, Why} -> Why end,
                        %% ?VV("Worker ~p exited ~p\n", [P, Status]),
                        Status
                    end || P <- Pids],
        true = lists:any(fun(X) -> X == ExitReason end, Statuses),
        receive
            {sink_final_result, MyRef, Val} ->
                exit({should_have_crashed, val, Val})
        after 50 ->
                %% We confirmed that everyone is dead, so waiting more
                %% than 0 msec for a bogus sink_final_result is overkill.
                ok
        end
    after
        process_flag(trap_exit, false)
    end.

-spec smoke_bp_farm1_test() -> term().
smoke_bp_farm1_test() ->
    Inputs = lists:seq(1,50),
    NWorkers = 5,
    Me = self(),
    MyRef = make_ref(),

    Farm1 = {bp_farm, 2,
             [{bp_seq, 1, fun bp_double/3, 22},
              {bp_seq, 1, fun bp_half/3, 77.4},
              {bp_seq, 1, fun bp_truncate/3, -2.22}], NWorkers},
    GetRes = fun() -> receive {sink_final_result, MyRef, Val} -> Val end end,

    {_FeederPid1, _WorkPids1} =
         skel:bp_do([{bp_seq,  2, fun bp_demo_identity/3, init_data_ignored},
                     Farm1,
                     {bp_sink, 2, fun bp_demo_sink/3, {Me,MyRef}}], Inputs),
    %% ?VV("Feeder ~p WorkPids ~p\n", [_FeederPid1, _WorkPids1]),
    Res1 = GetRes(),
    %% Results can arrive out of order, so we sort them for the match
    Inputs = lists:sort(Res1),

    %% Same thing but with farm at beginning of the workflow.
    {_FeederPid2, _WorkPids2} =
         skel:bp_do([Farm1,
                     {bp_seq,  2, fun bp_demo_identity/3, init_data_ignored},
                     Farm1,
                     {bp_sink, 2, fun bp_demo_sink/3, {Me,MyRef}}], Inputs),
    Res2 = GetRes(),
    %% Results can arrive out of order, so we sort them for the match
    Inputs = lists:sort(Res2),

    Farm2 = {bp_farm, 2,
             [Farm1,
              {bp_seq, 1, fun bp_double/3, 22},
              {bp_seq, 1, fun bp_double/3, 22},
              Farm1,
              {bp_seq, 1, fun bp_half/3, 77.4},
              {bp_seq, 1, fun bp_half/3, 77.4},
              %% Farm1,
              {bp_seq, 1, fun bp_truncate/3, -2.22}], NWorkers*2},
    {_FeederPid3, _WorkPids3} =
         skel:bp_do([Farm2,
                     {bp_seq,  2, fun bp_demo_identity/3, init_data_ignored},
                     Farm2,
                     {bp_sink, 2, fun bp_demo_sink/3, {Me,MyRef}}], Inputs),
    Res3 = GetRes(),
    %% Results can arrive out of order, so we sort them for the match
    Inputs = lists:sort(Res3),

    Inputsx = lists:duplicate(123, [q]),
    Farm0x = {bp_farm, 2,
              [{bp_seq, 1, fun bp_prepend/3, -1},
               {bp_seq, 1, fun bp_prepend/3, -2},
               {bp_seq, 1, fun bp_prepend/3, -3}], NWorkers},
    Farm1x = {bp_farm, 2,
              [{bp_seq, 1, fun bp_prepend/3, 1},
               Farm0x,
               {bp_seq, 1, fun bp_prepend/3, 2},
               {bp_seq, 1, fun bp_prepend/3, 3}], NWorkers},
    Farm2x = {bp_farm, 2,
              [Farm1x,
               {bp_seq, 1, fun bp_prepend/3, 10},
               {bp_seq, 1, fun bp_prepend/3, 11},
               Farm1x,
               {bp_seq, 1, fun bp_prepend/3, 12},
               {bp_seq, 1, fun bp_prepend/3, 13},
               Farm1x,
               {bp_seq, 1, fun bp_prepend/3, 14}], NWorkers*2},
    {_FeederPid4, _WorkPids4} =
        skel:bp_do([Farm2x,
                    {bp_sink, 2, fun bp_demo_sink/3, {Me,MyRef}}], Inputsx),
    Res3x = GetRes(),
    %% Results can arrive out of order, so we sort them for the match
    1 = length(lists:usort(Res3x)),

    ok.

poll_until_pid_dead(Pid) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ok;
        _ ->
            timer:sleep(10)
    end.

%% Simple state model: each stage keeps an additive accumulator and
%% the pid of our downstream.

-record(demo, {
          acc :: number()
         }).

bp_demo_identity(bp_init, _Data, _Ignore) ->
    %% ?VV("identity bp_init\n", []),
    {ok, #demo{acc=0}};
bp_demo_identity(bp_eoi, _, #demo{acc=_Acc}=S) ->
    %% ?VV("identity bp_eoi: Acc ~w\n", [_Acc]),
    {ok, S};
bp_demo_identity(bp_work, X, #demo{acc=Acc}=S) ->
    %% ?VV("identity bp_init: X ~p my links ~p\n", [X, process_info(self(), links)]),
    Res = X,
    %% ?VV("identity: X ~w -> ~w\n", [X, Res]),
    {done, [Res], S#demo{acc=Acc+Res}}.

bp_demo_sink(bp_init, {_,_}=MyParent, _Ignore) ->
    %% ?VV("sink bp_init: my links ~w\n", [process_info(self(), links)]),
    {ok, {[], MyParent}};
bp_demo_sink(bp_eoi, _, {Acc, {MyParentPid,Ref}}=State) ->
    %% ?VV("sink bp_eoi: Acc ~w\n", [Acc]),
    MyParentPid ! {sink_final_result, Ref, lists:reverse(Acc)},
    {ok, State};
bp_demo_sink(bp_work, Data, {Acc, MyParent}) ->
    %% ?VV("sink catchall: Data ~w\n", [Data]),
    {ok, {[Data|Acc], MyParent}}.

bp_double(bp_init, InitData, _Ignore) ->
    {ok, #demo{acc=InitData}};
bp_double(bp_eoi, _, S) ->
    {ok, S};
bp_double(bp_work, X, #demo{acc=Acc}=S) ->
    Res = X * 2,
    {done, [Res], S#demo{acc=Acc+Res}}.

bp_half(bp_init, InitData, _Ignore) ->
    {ok, #demo{acc=InitData}};
bp_half(bp_eoi, _, S) ->
    {ok, S};
bp_half(bp_work, X, #demo{acc=Acc}=S) ->
    Res = X / 2,
    {done, [Res], S#demo{acc=Acc+Res}}.

bp_truncate(bp_init, InitData, _Ignore) ->
    {ok, #demo{acc=InitData}};
bp_truncate(bp_eoi, _, S) ->
    {ok, S};
bp_truncate(bp_work, X, #demo{acc=Acc}=S) ->
    Res = trunc(X),
    {done, [Res], S#demo{acc=Acc+Res}}.

bp_verbose_sleep(bp_init, InitData, _Ignore) ->
    {ok, #demo{acc=InitData}};
bp_verbose_sleep(bp_eoi, _, S) ->
    {ok, S};
bp_verbose_sleep(bp_work, X, #demo{acc=SleepTime}=S) ->
    %% ?VV("verbose sleep: got ~p, my time = ~w\n", [X, SleepTime]),
    timer:sleep(SleepTime),
    %% ?VV("verbose sleep: done\n", []),
    Emits = if SleepTime == 0 -> [];
               true           -> [X]
            end,
    {done, Emits, S}.

-record(crash, {
          limit  :: non_neg_integer(),
          count  :: non_neg_integer(),
          reason :: term()
         }).

bp_crash_after(bp_init, {Limit,Reason}, _Ignore) ->
    %% ?VV("crash bp_init: my links ~p\n", [process_info(self(), links)]),
    {ok, #crash{limit=Limit, reason=Reason, count=0}};
bp_crash_after(bp_eoi, _, #crash{count=_Count}=S) ->
    %% ?VV("bp_crash_after: EIO count = ~w\n", [_Count]),
    {ok, S};
bp_crash_after(bp_work, X, #crash{limit=Limit, count=Count, reason=Reason}=S) ->
    %% ?VV("crash_after X ~w Count ~w reason ~w\n", [X, Count, Reason]),
    if Count >= Limit ->
            %% ?VV("crasher: gonna crash now, my links = ~w\n", [process_info(self(), links)]),
            exit(Reason);
       true -> ok
    end,
    {done, [X], S#crash{count=Count+1}}.

bp_prepend(bp_init, Data, _Ignore) ->
    {ok, Data};
bp_prepend(bp_eoi, _, S) ->
    {ok, S};
bp_prepend(bp_work, List, S) ->
    Res = [S|List],
    {done, [Res], S}.

-endif. % TEST
