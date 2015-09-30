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

-spec smoke_seq_test() -> term().
smoke_seq_test() ->
    %% timer:sleep(50),?VV("\n", []), ?VV("smoke_seq_test top\n", []),
    Inputs = [1,2,3,4,5],

    X1 = skel:do([{seq, fun identity/1},
                  {seq, fun double/1},
                  {seq, fun half/1},
                  {seq, fun truncate/1}], Inputs),
    Inputs = X1.

-spec smoke_pipe_test() -> term().
smoke_pipe_test() ->
    Inputs = [1,2,3,4,5],

    X1 = skel:do([{pipe,[{seq, fun identity/1},
                         {seq, fun double/1},
                         {seq, fun half/1},
                         {seq, fun truncate/1}]
                  }], Inputs),
    Inputs = X1.

-spec smoke_bp_sink_test() -> term().
smoke_bp_sink_test() ->
    Inputs = lists:seq(1,20),
    Me = self(),
    MyRef = make_ref(),

    %% timer:sleep(50),?VV("\n", []),?VV("smoke_bp_sink_test: top\n", []),
    _X = skel:bp_do([{bp_sink, fun bp_demo_sink/2, {Me, MyRef}}], Inputs),
    %% ?VV("smoke_bp_seq_test: X1 ~w\n", [X1]),
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
    _X = skel:bp_do([{bp_seq, fun bp_demo_identity/2, init_data_ignored},
                     {bp_seq, fun bp_double/2, 22},
                     {bp_seq, fun bp_half/2, 77.4},
                     {bp_seq, fun bp_truncate/2, -2.22},
                     {bp_sink, fun bp_demo_sink/2, {Me,MyRef}}], Inputs),
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

    timer:sleep(50),?VV("\n", []),?VV("smoke_bp_seq_sleep_test: top\n", []),
    _X = skel:bp_do([{bp_seq, fun bp_verbose_sleep/2, 50},
                     {bp_seq, fun bp_verbose_sleep/2, 70},
                     {bp_seq, fun bp_verbose_sleep/2, 90},
                     {bp_seq, fun bp_verbose_sleep/2, 0},
                     {bp_sink, fun bp_demo_sink/2, {Me,MyRef}}], Inputs),
    Res2 = receive
               {sink_final_result, MyRef, Val} ->
                   Val
           end,
    %% The last sleep with a 0 sleep time doesn't emit anything, so
    %% the sink doesn't collect anything.  But, nevertheless, we run
    %% to completion, yay!
    [] = Res2.

identity(X) ->
    X.

double(X) ->
    X * 2.

half(X) ->
    X / 2.

truncate(X) ->
    trunc(X).

%% Simple state model: each stage keeps an additive accumulator and
%% the pid of our downstream.

-record(demo, {
          acc :: number(),
          downstream :: pid()
         }).

bp_demo_identity({bp_init, _Data}, _Ignore) ->
    InFlight = 2,
    {InFlight, #demo{acc=0, downstream=undefined}};
bp_demo_identity({bp_downstream, DownStream}, S) ->
    {ok, S#demo{downstream=DownStream}};
bp_demo_identity(bp_eoi, #demo{acc=_Acc}=S) ->
    %% ?VV("identity bp_eoi: Acc ~w\n", [_Acc]),
    {ok, S};
bp_demo_identity(X, #demo{acc=Acc}=S) ->
    Res = X,
    %% ?VV("identity: X ~w -> ~w\n", [X, Res]),
    {[Res], S#demo{acc=Acc+Res}}.

bp_demo_sink({bp_init, {_,_}=MyParent}, _Ignore) ->
    %% ?VV("sink bp_init: ~w\n", [MyParent]),
    InFlight = 2,
    {InFlight, {[], MyParent}};
bp_demo_sink({bp_init, Bad}, _Ignore) ->
    ?VV("sink bp_init BAD: ~w\n", [Bad]),
    exit({expected_2_tuple_for_init,got,Bad});
bp_demo_sink(bp_eoi, {Acc, {MyParentPid,Ref}}=State) ->
    %% ?VV("sink bp_eoi: Acc ~w\n", [Acc]),
    MyParentPid ! {sink_final_result, Ref, lists:reverse(Acc)},
    {ok, State};
bp_demo_sink(Data, {Acc, MyParent}) ->
    %% ?VV("sink catchall: Data ~w\n", [Data]),
    {ok, {[Data|Acc], MyParent}}.

bp_double({bp_init, InitData}, _Ignore) ->
    InFlight = 2,
    {InFlight, #demo{acc=InitData, downstream=undefined}};
bp_double({bp_downstream, DownStream}, S) ->
    {ok, S#demo{downstream=DownStream}};
bp_double(bp_eoi, S) ->
    {ok, S};
bp_double(X, #demo{acc=Acc}=S) ->
    Res = X * 2,
    {[Res], S#demo{acc=Acc+Res}}.

bp_half({bp_init, InitData}, _Ignore) ->
    InFlight = 2,
    {InFlight, #demo{acc=InitData, downstream=undefined}};
bp_half({bp_downstream, DownStream}, S) ->
    {ok, S#demo{downstream=DownStream}};
bp_half(bp_eoi, S) ->
    {ok, S};
bp_half(X, #demo{acc=Acc}=S) ->
    Res = X / 2,
    {[Res], S#demo{acc=Acc+Res}}.

bp_truncate({bp_init, InitData}, _Ignore) ->
    InFlight = 2,
    {InFlight, #demo{acc=InitData, downstream=undefined}};
bp_truncate({bp_downstream, DownStream}, S) ->
    {ok, S#demo{downstream=DownStream}};
bp_truncate(bp_eoi, S) ->
    {ok, S};
bp_truncate(X, #demo{acc=Acc}=S) ->
    Res = trunc(X),
    {[Res], S#demo{acc=Acc+Res}}.

bp_verbose_sleep({bp_init, InitData}, _Ignore) ->
    InFlight = 2,
    {InFlight, #demo{acc=InitData, downstream=undefined}};
bp_verbose_sleep({bp_downstream, DownStream}, S) ->
    {ok, S#demo{downstream=DownStream}};
bp_verbose_sleep(bp_eoi, S) ->
    {ok, S};
bp_verbose_sleep(X, #demo{acc=SleepTime}=S) ->
    ?VV("verbose sleep: got ~p, my time = ~w\n", [X, SleepTime]),
    timer:sleep(SleepTime),
    ?VV("verbose sleep: done\n", []),
    Emits = if SleepTime == 0 -> [];
               true           -> [X]
            end,
    {Emits, S}.

-endif. % TEST
