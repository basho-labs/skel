%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @copyright 2015 Basho Technologies, Inc. (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module encapsulates all functions relating to data messages that
%%% are sent around the system.
%%%
%%% Data messages are an instance of an applicative functor, allowing various
%%% operations to happen in a very logical way.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_data).

-export([
         pure/1
        ,pure/2
        ,value/1
        ,identifiers/1
        ,make_data/3
        ,set_sender/1
        ,set_sender/2
        ]).

-include("skel.hrl").

-spec pure(any()) -> data_message().
%% @doc Formats a piece of data under `D' of any data type as a data message.
pure(D) ->
    pure(D, self()).

pure(D, FromPid) ->
  {data, FromPid, D, []}.

-spec value(data_message()) -> any().
%% @doc Extracts and returns a data message's value.
value({data, _FromPid, Value, _Identifiers}) ->
  Value.

-spec identifiers(data_message()) -> list().
%% @doc Extracts and returns a data message's identifiers.
identifiers({data, _FromPid, _Value, Identifiers}) ->
  Identifiers.

-spec make_data(pid(), any(), list()) -> data_message().
make_data(FromPid, Data, Identifiers) ->
    {FromPid, Data, Identifiers}.

-spec set_sender(data_message()) -> data_message().
set_sender(DataMessage) ->
    set_sender(DataMessage, self()).

-spec set_sender(data_message(), pid()) -> data_message().
set_sender({data, _, Value, Identifiers}, FromPid) ->
    {data, FromPid, Value, Identifiers}.
