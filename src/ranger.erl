-module(ranger).

%% Test App.
-export([start/0]).

%% API.
-export([upgrade/4]).

-record(state, {
  env :: cowboy_middleware:env(),
  method = undefined :: binary(),

  %% Handler.
  handler :: atom(),
  handler_state :: any(),

  %% Backend
  conn :: any(),
  ref :: any(),
  backend :: {atom(), list(), integer(), binary()},
  timeout = 5000 :: integer(),
  req_headers :: [{binary(), binary()}],

  res_status :: integer(),
  res_headers = [] :: [{iolist(), iolist()}],

  res_body_fun :: fun(),
  res_sent_bytes = 0 :: integer(),
  length = 0
}).

%% Test App.

start() ->
  ok = application:start(crypto),
  ok = application:start(sasl),
  ok = application:start(ranch),
  ok = application:start(cowboy),
  ok = application:start(ranger).

%% @doc Upgrade a HTTP request to the proxy protocol.
%%
%% You do not need to call this function manually. To upgrade to the proxy
%% protocol, you simply need to return <em>{upgrade, protocol, {@module}}</em>
%% in your <em>cowboy_http_handler:init/3</em> handler function.
-spec upgrade(Req, Env, module(), any())
  -> {ok, Req, Env} | {error, 500, Req}
  when Req::cowboy_req:req(), Env::cowboy_middleware:env().
upgrade(Req, Env, Handler, HandlerOpts) ->
  [Method, Headers] = cowboy_req:get([method, headers], Req),
  case erlang:function_exported(Handler, proxy_init, 2) of
    true ->
      try Handler:proxy_init(Req, HandlerOpts) of
        {ok, Req2, HandlerState} ->
          backend(Req2, #state{env = Env, method = Method, req_headers = Headers,
            handler = Handler, handler_state = HandlerState})
      catch Class:Reason ->
        error_terminate(Req, #state{handler = Handler, handler_state = HandlerOpts},
            Class, Reason, proxy_init, 3)
      end;
    false ->
      backend(Req, #state{env = Env, method = Method,
        handler = Handler})
  end.

%% select a backend. this can come from the router options or can be returned from the 'backend/2' method
backend(Req, State) ->
  case call(Req, State, backend) of
    no_call ->
      case fast_key:get(backend, State#state.env) of
        undefined ->
          next(Req, State, 502);
        Backend ->
          State2 = State#state{backend = normalize_backend(Backend)},
          next(Req, State2, fun timeout/2)
      end;
    {Backend, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           backend = normalize_backend(Backend)},
      next(Req2, State2, fun timeout/2)
  end.

%% TODO support more combos
normalize_backend({Proto, Host, Port, Path}) ->
  {Proto, Host, Port, Path};
normalize_backend({Proto, Host, Port}) when is_atom(Proto) andalso is_integer(Port) ->
  {Proto, Host, Port, <<>>};
normalize_backend({Host, Port}) when is_integer(Port) ->
  {http, Host, Port, <<>>};
normalize_backend({http, Host}) ->
  {http, Host, 80, <<>>};
normalize_backend({Proto, Host}) when is_atom(Proto) ->
  {Proto, Host, 443, <<>>}.

timeout(Req, State = #state{backend = Backend}) ->
  case call(Req, State, timeout, Backend) of
    no_call ->
      next(Req, State, fun open_connection/2);
    {Timeout, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           timeout = Timeout},
      next(Req2, State2, fun open_connection/2)
  end.

open_connection(Req, State = #state{backend = {Proto, Host, Port, _Path}, timeout = Timeout}) ->
  Opts = [
    {retry, 1}, %% TODO configure?
    {retry_timeout, Timeout},
    {type, type_from_proto(Proto)}
  ],
  case gun:open(Host, Port, Opts) of
    {ok, Conn} ->
      %% TODO handle a closed upstream connection
      next(Req, State#state{conn = Conn}, fun forwarded_header_prefix/2);
    {error, _Error} ->
      next(Req, State, 502)
  end.

type_from_proto(http) ->
  tcp;
type_from_proto(https) ->
  ssl;
type_from_proto(spdy) ->
  tcp_spdy;
type_from_proto(Proto) ->
  Proto.

forwarded_header_prefix(Req, State = #state{req_headers = ReqHeaders}) ->
  case call(Req, State, forwarded_header_prefix) of
    no_call ->
      next(Req, State, fun request_id/2);
    {Prefix, Req2, HandlerState} ->
      ForwardedHeaders = format_forwarded_headers(Req, Prefix),
      State2 = State#state{handler_state = HandlerState,
                           req_headers = ForwardedHeaders ++ ReqHeaders},
      next(Req2, State2, fun request_id/2)
  end.

request_id(Req, State = #state{req_headers = ReqHeaders, res_headers = ResHeaders}) ->
  case call(Req, State, request_id) of
    no_call ->
      next(Req, State, fun req_headers/2);
    {{Name, ID}, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           req_headers = [{Name, ID}|ReqHeaders],
                           res_headers = [{Name, ID}|ResHeaders]},
      next(Req2, State2, fun req_headers/2);
    {ID, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           req_headers = [{<<"x-request-id">>, ID}|ReqHeaders],
                           res_headers = [{<<"x-request-id">>, ID}|ResHeaders]},
      next(Req2, State2, fun req_headers/2)
  end.

req_headers(Req, State = #state{req_headers = ReqHeaders}) ->
  %% TODO filter headers - connection, host, content-length
  ReqHeaders2 = filter_headers(ReqHeaders, []),
  case call(Req, State, req_headers, ReqHeaders2) of
    no_call ->
      next(Req, State#state{req_headers = ReqHeaders2}, fun init_request/2);
    {ReqHeaders3, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           req_headers = ReqHeaders3},
      next(Req2, State2, fun init_request/2)
  end.

filter_headers([], Acc) ->
  Acc;
filter_headers([{<<"host">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([{<<"connection">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([{<<"content-length">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([Header|Headers], Acc) ->
  filter_headers(Headers, [Header|Acc]).

init_request(Req, State = #state{conn = Conn, method = Method, backend = {_, _, _, BasePath}, req_headers = Headers}) ->
  {Parts, Req2} = cowboy_req:path_info(Req),
  Path = path_join(BasePath, Parts),
  %% TODO add the cowboy_req:qs
  io:format("~n~p ~p~n~p~n", [Method, Path, Headers]),
  Ref = gun:request(Conn, Method, Path, Headers),
  next(Req2, State#state{ref = Ref}, fun req_body/2).

path_join(<<>>, Parts) ->
  [<<"/">>, io_join(lists:reverse(Parts), <<"/">>, [])];
path_join(<<"/">>, Parts) ->
  [<<"/">>, io_join(lists:reverse(Parts), <<"/">>, [])];
path_join(BasePath, Parts) ->
  io_join(lists:reverse([BasePath|Parts]), <<"/">>, []).

io_join([], _Sep, Acc) ->
  Acc;
io_join([Item], _Sep, Acc) ->
  [Item, Acc];
io_join([Item|Items], Sep, Acc) ->
  io_join(Items, Sep, [Sep, Item, Acc]).

req_body(Req, State = #state{conn = Conn, ref = Ref}) ->
  case cowboy_req:has_body(Req) of
    true ->
      case exported(Req, State, req_body, 3) of
        true ->
          %% TODO should we catch the errors?
          %% TODO expose max body size variable
          {ok, Body, Req2} = cowboy_req:body(Req),
          {Body2, Req3, State2} = call(Req2, State, req_body, Body),
          ok = gun:data(Conn, Ref, fin, Body2),
          next(Req3, State2, fun res_status/2);
        _ ->
          next(Req, State, fun chunk_req_body/2)
      end;
    _ ->
      next(Req, State, fun res_status/2)
  end.

chunk_req_body(Req, State = #state{conn = Conn, ref = Ref}) ->
  case cowboy_req:stream_body(Req) of
    {ok, Body, Req2} ->
      ok = gun:data(Conn, Ref, nofin, Body),
      chunk_req_body(Req2, State);
    {done, Req2} ->
      ok = gun:date(Conn, Ref, fin, <<>>),
      next(Req2, State, fun res_status/2);
    {error, _Reason} ->
      %% TODO send back a better error message
      next(Req, State, 400)
  end.

res_status(Req, State = #state{conn = Conn, ref = Ref, timeout = _Timeout, res_headers = ResHeaders}) ->
  receive
    {gun_response, Conn, Ref, _Fin, Status, Headers} ->
      %% TODO handle if fin
      next(Req, State#state{res_status = Status, res_headers = Headers ++ ResHeaders}, fun res_headers/2)
  %% after Timeout TODO
  end.

res_headers(Req, State = #state{res_status = ResStatus, res_headers = ResHeaders}) ->
  %% TODO filter the response headers - connection, content-length, date?

  {ManipulatedHeaders, Req2, State2} = case call(Req, State, res_headers, ResHeaders) of
    no_call ->
      {ResHeaders, Req, State};
    Result ->
      Result
  end,
  {ok, Req3} = cowboy_req:chunked_reply(ResStatus, ManipulatedHeaders, Req2),
  next(Req3, State2, fun get_res_body/2).

get_res_body(Req, State) ->
  case exported(Req, State, res_body, 3) of
    true ->
      %% TODO
      ok;
    _ ->
      stream_body(Req, State)
  end.

stream_body(Req, State = #state{conn = Conn, ref = Ref, timeout = _Timeout}) ->
  receive
    {gun_data, Conn, Ref, nofin, Data} ->
      ok = cowboy_req:chunk(Data, Req),
      stream_body(Req, State);
    {gun_data, Conn, Ref, fin, Data} ->
      ok = cowboy_req:chunk(Data, Req),
      terminate(Req, State)
  %% after Timeout TODO
  end.

%% Formatting
format_forwarded_headers(Req, Prefix) ->
  %% TODO this is wrong. fix this to use the values from the upstream request
  [Host, Port, Path] = cowboy_req:get([host, port, path], Req),
  [
    {<<Prefix/binary, "-proto">>, <<"http">>}, %% TODO
    {<<Prefix/binary, "-host">>, Host},
    {<<Prefix/binary, "-port">>, integer_to_binary(Port)},
    {<<Prefix/binary, "-path">>, Path}
  ].

%% Protocol

call(Req, State=#state{handler=Handler, handler_state=HandlerState}, Callback) ->
  case erlang:function_exported(Handler, Callback, 2) of
    true ->
      try
        Handler:Callback(Req, HandlerState)
      catch Class:Reason ->
        error_terminate(Req, State, Class, Reason, Callback, 2)
      end;
    false ->
      no_call
  end.

call(Req, State=#state{handler=Handler, handler_state=HandlerState}, Callback, Arg) ->
  case erlang:function_exported(Handler, Callback, 3) of
    true ->
      try
        Handler:Callback(Arg, Req, HandlerState)
      catch Class:Reason ->
        error_terminate(Req, State, Class, Reason, Callback, 3)
      end;
    false ->
      no_call
  end.

exported(_Req, _State = #state{handler = Handler}, Callback, Arity) ->
  erlang:function_exported(Handler, Callback, Arity).

next(Req, State, Next) when is_function(Next) ->
  Next(Req, State);
next(Req, State, StatusCode) when is_integer(StatusCode) ->
  respond(Req, State, StatusCode).

respond(Req, State, StatusCode) ->
  {ok, Req2} = cowboy_req:reply(StatusCode, Req),
  terminate(Req2, State).

terminate(Req, State = #state{env = Env, conn = Conn}) ->
  proxy_terminate(Req, State),
  ok = gun:close(Conn),
  {ok, Req, [{result, ok}|Env]}.

error_terminate(Req, State=#state{handler=Handler, handler_state=HandlerState},
		Class, Reason, Callback, Arity) ->
  proxy_terminate(Req, State),
  cowboy_req:maybe_reply(500, Req),
  erlang:Class([
    {reason, Reason},
    {mfa, {Handler, Callback, Arity}},
    {stacktrace, erlang:get_stacktrace()},
    {req, cowboy_req:to_list(Req)},
    {state, HandlerState}
  ]).

proxy_terminate(Req, #state{handler=Handler, handler_state=HandlerState}) ->
  case erlang:function_exported(Handler, proxy_terminate, 2) of
    true -> ok = Handler:proxy_terminate(
      cowboy_req:lock(Req), HandlerState);
    false -> ok
  end.
