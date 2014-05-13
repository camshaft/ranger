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
  backend :: {atom(), list(), integer(), binary()},
  timeout :: integer(),
  req_headers :: [{binary(), binary()}],
  req_body :: binary(),

  res_status :: integer(),
  res_headers = [] :: [{iolist(), iolist()}],

  res_body_fun :: fun(),
  res_sent_bytes = 0 :: integer(),
  length = 0
}).

-define(DEFAULT_TIMEOUT, 5000).

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
          backend(Req2, #state{env=Env, method=Method, req_headers=Headers,
            handler=Handler, handler_state=HandlerState})
      catch Class:Reason ->
        error_terminate(Req, #state{handler = Handler, handler_state = HandlerOpts},
            Class, Reason, proxy_init, 3)
      end;
    false ->
      backend(Req, #state{env=Env, method=Method,
        handler=Handler})
  end.

backend(Req, State) ->
  case call(Req, State, backend) of
    no_call ->
      %% TODO pull from the protocol opts
      next(Req, State, 502);
    {{Proto, Host, Port, Path}, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           backend={Proto, Host, Port, Path}},
      next(Req2, State2, fun timeout/2);
    {{Proto, Host, Port}, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           backend={Proto, Host, Port, <<>>}},
      next(Req2, State2, fun timeout/2);
    {{Host, Port}, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           backend={http, Host, Port, <<>>}},
      next(Req2, State2, fun timeout/2)
  end.

timeout(Req, State=#state{backend=Backend}) ->
  case call(Req, State, timeout, Backend) of
    no_call ->
      next(Req, State#state{timeout=?DEFAULT_TIMEOUT}, fun short_circuit/2);
    {Timeout, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           timeout=Timeout},
      next(Req2, State2, fun short_circuit/2)
  end.

short_circuit(Req, State) ->
  case call(Req, State, short_circuit) of
    no_call ->
      next(Req, State, fun open_connection/2);
    {false, Req, State} ->
      next(Req, State, fun open_connection/2);
    {true, Req, State} ->
      {ok, Req, State}
  end.

open_connection(Req, State=#state{backend={_Proto, Host, Port, _Path}, timeout=Timeout}) ->
  %% TODO connect over ssl if it's https
  %% TODO expose a function to manipulate the tcp options
  Opts = [binary, {packet, http_bin}, {packet_size, 1024 * 1024}, {recbuf, 1024 * 1024}, {active, once}, {reuseaddr, true}],
  case gen_tcp:connect(Host, Port, Opts, Timeout) of
    {ok, Conn} ->
      %% TODO handle a closed upstream connection
      next(Req, State#state{conn=Conn}, fun init_request/2);
    {error, _Error} ->
      next(Req, State, 502)
  end.

init_request(Req, State=#state{method=Method}) ->
  {Version, Req2} = cowboy_req:version(Req),
  {Message, Req4} = case cowboy_req:path_info(Req2) of
    {<<"/", _>> = P, Req3} ->
      {format_request(Method, P, Version), Req3};
    {undefined, Req3} ->
      {format_request(Method, <<"/">>, Version), Req3};
    {Path, Req3} ->
      {format_request(Method, [<<"/">>, Path], Version), Req3}
  end,
  send(Req4, State, Message, fun forwarded_header_prefix/2).

forwarded_header_prefix(Req, State=#state{backend=Backend, req_headers=ReqHeaders}) ->
  case call(Req, State, forwarded_header_prefix) of
    no_call ->
      next(Req, State, fun request_id/2);
    {Prefix, Req2, HandlerState} ->
      ForwardedHeaders = format_forwarded_headers(Backend, Prefix),
      State2 = State#state{handler_state=HandlerState,
                           req_headers=ForwardedHeaders++ReqHeaders},
      next(Req2, State2, fun request_id/2)
  end.

request_id(Req, State=#state{req_headers=ReqHeaders, res_headers=ResHeaders}) ->
  case call(Req, State, request_id) of
    no_call ->
      next(Req, State, fun req_headers/2);
    {ID, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           req_headers=[{<<"x-request-id">>, ID}|ReqHeaders],
                           res_headers=[{<<"x-request-id">>, ID}|ResHeaders]},
      next(Req2, State2, fun req_headers/2)
  end.

req_headers(Req, State=#state{req_headers=ReqHeaders}) ->
  case call(Req, State, req_headers, lists:keydelete(<<"connection">>, 1, ReqHeaders)) of
    no_call ->
      next(Req, State, fun send_req_headers/2);
    {ReqHeaders2, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           req_headers=ReqHeaders2},
      next(Req2, State2, fun send_req_headers/2)
  end.

send_req_headers(Req, State=#state{req_headers=ReqHeaders, backend=Backend}) ->
  FormattedHeaders = [case Name of
    <<"host">> ->
      format_header(Name, format_host_header(Backend));
    _ ->
      format_header(Name, Value)
  end || {Name, Value} <- ReqHeaders],

  send(Req, State, [FormattedHeaders, <<"\r\n">>], fun req_body/2).

req_body(Req, State) ->
  case cowboy_req:has_body(Req) of
    true ->
      case exported(Req, State, req_body, 3) of
        true ->
          %% TODO should we catch the errors?
          %% TODO expose max body size variable
          {ok, Body, Req2} = cowboy_req:body(Req),
          {Body2, Req3, State2} = call(Req2, State, req_body, Body),
          next(Req3, State2#state{req_body = Body2}, fun send_req_body/2);
        _ ->
          next(Req, State, fun chunk_req_body/2)
      end;
    _ ->
      next(Req, State, fun get_res_status/2)
  end.

send_req_body(Req, State = #state{req_body = Body}) ->
  %% TODO can we send all of the body at once and have it buffer automatically or do we need to chunk it?
  send(Req, State, Body, fun get_res_status/2).

chunk_req_body(Req, State) ->
  %% TODO expose max body size variable
  case cowboy_req:stream_body(Req) of
    {ok, Body, Req2} ->
      send(Req2, State, Body, fun chunk_req_body/2);
    {done, Req2} ->
      next(Req2, State, fun get_res_status/2);
    {error, _Reason} ->
      %% TODO send back a better error message
      next(Req, State, 400)
  end.

get_res_status(Req, State = #state{conn = Conn}) ->
  inet:setopts(Conn, [{active, once}]),
  receive
    {http, Conn, {http_response, _HttpVersion, HttpRespCode, _ResStatus}} ->
      next(Req, State#state{res_status = HttpRespCode}, fun get_res_headers/2);
    {tcp_closed, Conn} ->
      next(Req, State, 502)
  %% TODO add a response timeout
  % after Timeout
  end.

get_res_headers(Req, State = #state{conn = Conn, res_headers = ResHeaders}) ->
  %% TODO how do we make it not forward the headers onto the client?
  inet:setopts(Conn, [{active, once}]),
  receive
    {http, Conn, {http_header, _, 'Connection', _, _Val}} ->
      get_res_headers(Req, State);
    {http, Conn, {http_header, _, 'Content-Length', _, Length}} ->
      get_res_headers(Req, State#state{length = binary_to_integer(Length)});
    {http, Conn, {http_header, _, 'Date', _, _Val}} ->
      get_res_headers(Req, State);
    {http, Conn, {http_header, _, Field, _, Value}} ->
      NewResHeaders = [{atom_to_binary(Field), Value}|ResHeaders],
      get_res_headers(Req, State#state{res_headers = NewResHeaders});
    {http, Conn, http_eoh} ->
      send_res_headers(Req, State)
  %% TODO add a response timeout
  % after Timeout
  end.

send_res_headers(Req, State = #state{res_status = ResStatus, res_headers = ResHeaders}) ->
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
      ok;
    _ ->
      stream_body(Req, State)
  end.

%% TODO handle chunked encoding

stream_body(Req, State = #state{res_sent_bytes = SentBytes, length = Length}) when SentBytes >= Length ->
  terminate(Req, State);
stream_body(Req, State = #state{conn = Conn, res_sent_bytes = SentBytes}) ->
  inet:setopts(Conn, [{active, once}, {packet, raw}]),
  receive
    {tcp, Conn, Data} ->
      ok = cowboy_req:chunk(Data, Req),
      get_res_body(Req, State#state{res_sent_bytes = SentBytes + byte_size(Data)});
    {tcp_closed, Conn} ->
      terminate(Req, State)
  %% TODO add a response timeout
  % after Timeout
  end.

% internal

%% Formatting

format_request(Method, undefined, Version) ->
  format_request(Method, <<"/">>, Version);
format_request(Method, Path, Version) ->
  [Method, <<" ">>, Path, <<" ">>, format_version(Version), <<"\r\n">>].

format_version({1, 1}) ->
  <<"HTTP/1.1">>;
format_version('HTTP/1.1') ->
  <<"HTTP/1.1">>;
format_version(_V) ->
  <<"HTTP/1.0">>.

format_header(Name, Value) ->
  [Name, <<": ">>, Value, <<"\r\n">>].

format_host_header({_Proto, Host, Port, _Path}) ->
  <<(list_to_binary(Host))/binary, ":", (integer_to_binary(Port))/binary>>.

format_forwarded_headers({Proto, Host, Port, Path}, Prefix) ->
  [
    {<<Prefix/binary, "-proto">>, list_to_binary(atom_to_list(Proto))},
    {<<Prefix/binary, "-host">>, list_to_binary(Host)},
    {<<Prefix/binary, "-port">>, integer_to_binary(Port)},
    {<<Prefix/binary, "-path">>, Path}
  ].

atom_to_binary(Val) when is_atom(Val) ->
  list_to_binary(atom_to_list(Val));
atom_to_binary(Val) when is_binary(Val) ->
  Val;
atom_to_binary(Val) when is_list(Val) ->
  list_to_binary(Val).

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

exported(_Req, _State = #state{handler=Handler}, Callback, Arity) ->
  erlang:function_exported(Handler, Callback, Arity).

next(Req, State, Next) when is_function(Next) ->
  Next(Req, State);
next(Req, State, StatusCode) when is_integer(StatusCode) ->
  respond(Req, State, StatusCode).

send(Req, State = #state{conn = Conn}, Data, OnOK) ->
  case gen_tcp:send(Conn, Data) of
    ok ->
      OnOK(Req, State);
    {error, _Error} ->
      next(Req, State, 502)
  end.

respond(Req, State, StatusCode) ->
  {ok, Req2} = cowboy_req:reply(StatusCode, Req),
  terminate(Req2, State).

terminate(Req, State=#state{env=Env, conn = Conn}) ->
  proxy_terminate(Req, State),
  gen_tcp:close(Conn),
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
