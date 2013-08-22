%% @private
-module(ranger_app).
-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% API.

start(_Type, _Args) ->
  Dispatch = cowboy_router:compile([
    {'_', [
      {"/", ranger_example, []}
    ]}
  ]),

  {ok, _} = cowboy:start_http(http, 100, [{
    port, simple_env:get_integer("PORT", 8080)}
  ], [
    {env, [{dispatch, Dispatch}]}
  ]),

  ranger_sup:start_link().

stop(_State) ->
  ok.
