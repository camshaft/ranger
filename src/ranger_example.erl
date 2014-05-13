-module(ranger_example).

-export([init/3]).
-export([proxy_init/2]).
-export([backend/2]).
-export([timeout/3]).
-export([forwarded_header_prefix/2]).
-export([request_id/2]).
-export([req_headers/3]).

-define(CONNECT_TIMEOUT, 5000).

init(_, _Req, _Opts) ->
  {upgrade, protocol, ranger}.

proxy_init(Req, Opts) ->
  {ok, Req, Opts}.

backend(Req, State) ->
  {{http, "oc-api-mock.herokuapp.com", 80, <<"/">>}, Req, State}.

timeout(_Host, Req, State) ->
  {?CONNECT_TIMEOUT, Req, State}.

forwarded_header_prefix(Req, State) ->
  {<<"x-orig">>, Req, State}.

request_id(Req, State) ->
  {<<"request-id">>, Req, State}.

req_headers(Headers, Req, State) ->
  {[{<<"x-test">>, <<"123">>}|Headers], Req, State}.

% req_body(Body, Req, State) ->
%   {Body, Req, State}.

% res_headers(Headers, Req, State) ->
%   {Headers, Req, State}.

% res_body(Body, Req, State) ->
%   {Body, Req, State}.
