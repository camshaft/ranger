-module(ranger).

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
  keep_alive = false :: boolean(),
  backend :: {atom(), list(), integer(), binary()},
  timeout = 10000 :: integer(),
  req_headers :: [{binary(), binary()}],

  res_status :: integer(),
  res_headers = [] :: [{iolist(), iolist()}],
  res_has_body :: boolean(),
  res_chunked :: boolean()
}).

-define(HANDLE_ERROR(State),
  {error, ExitStatus, HandlerReq} when is_integer(ExitStatus) ->
    next(HandlerReq, State, ExitStatus)
).

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
          backend(Req2, #state{env = Env, method = Method, req_headers = filter_headers(Headers, []),
            handler = Handler, handler_state = HandlerState})
      catch Class:Reason ->
        error_terminate(Req, #state{handler = Handler, handler_state = HandlerOpts},
            Class, Reason, proxy_init, 3)
      end;
    false ->
      backend(Req, #state{env = Env, method = Method,
        handler = Handler})
  end.

%% Init.

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
    ?HANDLE_ERROR(State);
    {Backend, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           backend = normalize_backend(Backend)},
      next(Req2, State2, fun timeout/2)
  end.

timeout(Req, State = #state{backend = Backend}) ->
  case call(Req, State, timeout, Backend) of
    no_call ->
      next(Req, State, fun open_connection/2);
    ?HANDLE_ERROR(State);
    {Timeout, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           timeout = Timeout},
      next(Req2, State2, fun open_connection/2)
  end.

open_connection(Req, State = #state{backend = {Conn, Path}}) when is_pid(Conn) ->
  next(Req, State#state{conn = Conn, keep_alive = true, backend = {pid, pid, pid, Path}}, fun req_path/2);
open_connection(Req, State = #state{backend = {Proto, Host, Port, _Path}, timeout = Timeout}) ->
  Opts = [
    {retry, fast_key:get(retry, State#state.env, 1)},
    {retry_timeout, Timeout},
    {type, type_from_proto(Proto)}
  ],
  case gun:open(Host, Port, Opts) of
    {ok, Conn} ->
      next(Req, State#state{conn = Conn}, fun req_path/2);
    {error, Reason} ->
      error_terminate(Req, State, error, Reason, open_connection, 2, 502)
  end.

%% Request.

req_path(Req, State = #state{backend = {_, _, _, BasePath}}) ->
  {Parts, Req2} = cowboy_req:path_info(Req),
  Path = path_join(Parts),
  {Path2, Req2, State2} = case call(Req, State, req_path, Path) of
    no_call ->
      {Path, Req, State};
    {Info, R2, HandlerState} ->
      {Info, R2, State#state{handler_state = HandlerState}}
  end,
  Req3 = cowboy_req:set_meta(backend_path, path_join_base(BasePath, Path2), Req2),
  Req4 = cowboy_req:set_meta(base_path, Path2, Req3),
  next(Req4, State2, fun append_qs/2).

append_qs(Req, State) ->
  {Path, _} = cowboy_req:meta(backend_path, Req),
  case cowboy_req:qs(Req) of
    {<<>>, _} ->
      Req2 = cowboy_req:set_meta(backend_path_qs, Path, Req),
      next(Req2, State, fun forwarded_header_prefix/2);
    {QS, _} ->
      Req2 = cowboy_req:set_meta(backend_path_qs, <<Path/binary, "?", QS/binary>>, Req),
      next(Req2, State, fun forwarded_header_prefix/2)
  end.

forwarded_header_prefix(Req, State = #state{req_headers = ReqHeaders}) ->
  case call(Req, State, forwarded_header_prefix) of
    no_call ->
      next(Req, State, fun request_id/2);
    ?HANDLE_ERROR(State);
    {{_, _, _, _} = Headers, Req2, HandlerState} ->
      ForwardedHeaders = format_forwarded_headers(Req, Headers),
      State2 = State#state{handler_state = HandlerState,
                           req_headers = ForwardedHeaders ++ ReqHeaders},
      next(Req2, State2, fun request_id/2);
    {Prefix, Req2, HandlerState} when is_binary(Prefix) ->
      ForwardedHeaders = format_forwarded_headers(Req, {
        <<Prefix/binary, "-proto">>,
        <<Prefix/binary, "-host">>,
        <<Prefix/binary, "-port">>,
        <<Prefix/binary, "-path">>
      }),
      State2 = State#state{handler_state = HandlerState,
                           req_headers = ForwardedHeaders ++ ReqHeaders},
      next(Req2, State2, fun request_id/2)
  end.

request_id(Req, State = #state{req_headers = ReqHeaders, res_headers = ResHeaders}) ->
  case call(Req, State, request_id) of
    no_call ->
      next(Req, State, fun req_headers/2);
    ?HANDLE_ERROR(State);
    {{ReqName, ResName, ID}, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           req_headers = [{ReqName, ID}|ReqHeaders],
                           res_headers = [{ResName, ID}|ResHeaders]},
      next(Req2, State2, fun req_headers/2);
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
  case call(Req, State, req_headers, ReqHeaders) of
    no_call ->
      next(Req, State, fun init_request/2);
    ?HANDLE_ERROR(State);
    {ReqHeaders2, Req2, HandlerState} ->
      State2 = State#state{handler_state = HandlerState,
                           req_headers = ReqHeaders2},
      next(Req2, State2, fun init_request/2)
  end.

init_request(Req, State = #state{conn = Conn, method = Method, req_headers = Headers}) ->
  {Path, _} = cowboy_req:meta(backend_path_qs, Req),
  %% io:format("~p ~p~n~n~p~n", [Method, Path, Headers]),
  case cowboy_req:has_body(Req) of
    true ->
      Ref = gun:request(Conn, Method, Path, [{<<"transfer-encoding">>, <<"chunked">>}|Headers]),
      next(Req, State#state{ref = Ref}, fun req_body/2);
    _ ->
      Ref = gun:request(Conn, Method, Path, Headers),
      next(Req, State#state{ref = Ref}, fun res_status/2)
  end.

req_body(Req, State) ->
  case exported(Req, State, req_body, 3) of
    true ->
      next(Req, State, fun transform_req_body/2);
    _ ->
      next(Req, State, fun send_req_body/2)
  end.

transform_req_body(Req, State = #state{conn = Conn, ref = Ref}) ->
  MaxLength = fast_key:get(maxlength, State#state.env, 8000000),
  case cowboy_req:body(MaxLength, Req) of
    {ok, Body, Req2} ->
      {Body2, Req3, HandlerState} = call(Req2, State, req_body, Body),
      ok = gun:data(Conn, Ref, fin, Body2),
      next(Req3, State#state{handler_state = HandlerState}, fun res_status/2);
    {error, badlength} ->
      next(Req, State, 413)
  end.

send_req_body(Req, State = #state{conn = Conn, ref = Ref}) ->
  case cowboy_req:body(Req) of
    {ok, Body, Req2} ->
      ok = gun:data(Conn, Ref, fin, Body),
      next(Req2, State, fun res_status/2);
    {error, Reason} ->
      error_terminate(Req, State, error, Reason, chunk_req_body, 2, 400)
  end.

%% Response.

res_status(Req, State = #state{conn = Conn, ref = Ref, timeout = Timeout, res_headers = ResHeaders}) ->
  case gun:await(Conn, Ref, Timeout) of
    {response, Fin, Status, Headers} ->
      Chunked = fast_key:get(<<"transfer-encoding">>, Headers) =:= <<"chunked">>,
      next(Req, State#state{res_status = Status,
                            res_headers = filter_headers(Headers, ResHeaders),
                            res_has_body = Fin =:= nofin,
                            res_chunked = Chunked}, fun res_headers/2);
    {error, timeout} ->
      next(Req, State, 504);
    {error, {closed, _}} ->
      %% TODO report this
      terminate(Req, State);
    {error, Reason} ->
      error_terminate(Req, State, error, Reason, res_status, 2, 502)
  end.

res_headers(Req, State = #state{res_headers = ResHeaders}) ->
  case call(Req, State, res_headers, ResHeaders) of
    no_call ->
      next(Req, State, fun reply/2);
    ?HANDLE_ERROR(State);
    {FilteredHeaders, Req2, HandlerState} ->
      next(Req2, State#state{handler_state = HandlerState,
                             res_headers = FilteredHeaders}, fun reply/2)
  end.

reply(Req, State = #state{res_has_body = true}) ->
  next(Req, State, fun res_body/2);
reply(Req, State = #state{res_status = Status, res_headers = Headers}) ->
  {ok, Req2} = cowboy_req:reply(Status, Headers, Req),
  terminate(Req2, State).

res_body(Req, State = #state{res_status = Status, res_headers = Headers, res_chunked = true}) ->
  case exported(Req, State, res_body, 3) of
    true ->
      next(Req, State#state{res_chunked = false}, fun transform_res_body/2);
    _ ->
      {ok, Req2} = cowboy_req:chunked_reply(Status, Headers, Req),
      next(Req2, State, fun chunk_res_body/2)
  end;
res_body(Req, State) ->
  next(Req, State, fun transform_res_body/2).

transform_res_body(Req, State = #state{conn = Conn, ref = Ref, timeout = Timeout}) ->
  case gun:await_body(Conn, Ref, Timeout) of
    {ok, Body} ->
      case call(Req, State, res_body, Body) of
        no_call ->
          send(Req, State, Body, fun terminate/2);
        ?HANDLE_ERROR(State);
        {Body2, Req2, HandlerState} ->
          send(Req2, State#state{handler_state = HandlerState}, Body2, fun terminate/2)
      end;
    {error, timeout} ->
      next(Req, State, 504);
    {error, Reason} ->
      error_terminate(Req, State, error, Reason, res_status, 2, 502)
  end.

chunk_res_body(Req, State = #state{conn = Conn, ref = Ref, timeout = Timeout}) ->
  case gun:await(Conn, Ref, Timeout) of
    {data, nofin, Data} ->
      send(Req, State, Data, fun chunk_res_body/2);
    {data, fin, Data} ->
      send(Req, State, Data, fun terminate/2);
    {push, AssocToStreamRef, Method, Host, Path, Headers} ->
      error_logger:error_msg("Ignored PUSH ~p ~p ~p ~p ~p~n",
		[AssocToStreamRef, Method, Host, Path, Headers]),
      chunk_res_body(Req, State);
    {error, timeout} ->
      chunk_res_body(Req, State);
    {error, Reason} ->
      error_terminate(Req, State, error, Reason, res_status, 2, 502)
  end.

%% Formatting.

format_forwarded_headers(Req, {Proto, Host, Port, Path}) ->
  format_forwarded_proto(Req, Proto) ++
  format_forwarded_host(Req, Host) ++
  format_forwarded_port(Req, Port) ++
  format_forwarded_path(Req, Path).

format_forwarded_proto(Req, Header) ->
  case cowboy_req:header(Header, Req) of
    {undefined, _} ->
      case cowboy_req:get([transport], Req) of
        [ranch_tcp] ->
          [{Header, <<"http">>}];
        _ ->
          [{Header, <<"https">>}]
      end;
    _ ->
      []
  end.

format_forwarded_host(Req, Header) ->
  case cowboy_req:header(Header, Req) of
    {undefined, _} ->
      [Host] = cowboy_req:get([host], Req),
      [{Header, Host}];
    _ ->
      []
  end.

format_forwarded_port(Req, Header) ->
  case cowboy_req:header(Header, Req) of
    {undefined, _} ->
      Port = get_port(Req),
      [{Header, integer_to_binary(Port)}];
    _ ->
      []
  end.

get_port(Req) ->
  case cowboy_req:get([port], Req) of
    [undefined] -> 443;
    [Port] -> Port
  end.

format_forwarded_path(Req, Header) ->
  case cowboy_req:header(Header, Req) of
    {undefined, _} ->
      [Path] = cowboy_req:get([path], Req),
      {BackendPath, _} = cowboy_req:meta(base_path, Req),
      [{Header, binary:part(Path, {0, backend_size(Path, BackendPath)})}];
    _ ->
      []
  end.

backend_size(Path, BackendPath) ->
  case BackendPath of
    <<>> ->
      byte_size(Path);
    <<"/">> ->
      byte_size(Path);
    _ ->
      byte_size(Path) - byte_size(BackendPath)
  end - case binary:last(Path) of
    $/ -> 1;
    _ -> 0
  end.

%% internal.

path_join(undefined) ->
  <<>>;
path_join(Parts) ->
  list_to_binary([<<"/">>, io_join(lists:reverse(Parts), <<"/">>, [])]).

path_join_base(BasePath, <<>>) ->
  BasePath;
path_join_base(<<>>, Path) ->
  Path;
path_join_base(<<"/">>, Path) ->
  Path;
path_join_base(BasePath, Path) ->
  <<BasePath/binary, Path/binary>>.

io_join([], _Sep, Acc) ->
  Acc;
io_join([Item], _Sep, Acc) ->
  [Item, Acc];
io_join([Item|Items], Sep, Acc) ->
  io_join(Items, Sep, [Sep, Item, Acc]).

filter_headers([], Acc) ->
  Acc;
filter_headers([{<<"host">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([{<<"connection">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([{<<"content-length">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([{<<"date">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([{<<"transfer-encoding">>, _}|Headers], Acc) ->
  filter_headers(Headers, Acc);
filter_headers([Header|Headers], Acc) ->
  filter_headers(Headers, [Header|Acc]).

%% TODO support more combos
normalize_backend(Conn) when is_pid(Conn) ->
  {Conn, <<"/">>};
normalize_backend({Conn, Path}) when is_pid(Conn) ->
  {Conn, Path};
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

type_from_proto(http) ->
  tcp;
type_from_proto(https) ->
  ssl;
type_from_proto(spdy) ->
  tcp_spdy;
type_from_proto(Proto) ->
  Proto.

%% Protocol.

call(Req, State = #state{handler = Handler, handler_state = HandlerState}, Callback) ->
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

call(Req, State = #state{handler = Handler, handler_state = HandlerState}, Callback, Arg) ->
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

send(Req, State = #state{res_chunked = true}, Data, Next) ->
  case cowboy_req:chunk(Data, Req) of
    ok ->
      next(Req, State, Next);
    {error, closed} ->
      terminate(Req, State);
    {error, Reason} ->
      error_terminate(Req, State, error, Reason, send, 4)
  end;
send(Req, State = #state{res_status = Status, res_headers = Headers}, Data, Next) ->
  {ok, Req2} = cowboy_req:reply(Status, Headers, Data, Req),
  next(Req2, State, Next).

exported(_Req, _State = #state{handler = Handler}, Callback, Arity) ->
  erlang:function_exported(Handler, Callback, Arity).

next(Req, State, Next) when is_function(Next) ->
  Next(Req, State);
next(Req, State, StatusCode) when is_integer(StatusCode) ->
  respond(Req, State, StatusCode).

respond(Req, State, StatusCode) ->
  {ok, Req2} = cowboy_req:reply(StatusCode, Req),
  terminate(Req2, State).

terminate(Req, State = #state{env = Env, keep_alive = true}) ->
  proxy_terminate(Req, State),
  {ok, Req, Env};
terminate(Req, State = #state{env = Env, conn = Conn}) when is_pid(Conn) ->
  proxy_terminate(Req, State),
  ok = gun:shutdown(Conn),
  {ok, Req, Env};
terminate(Req, State = #state{env = Env}) ->
  proxy_terminate(Req, State),
  {ok, Req, Env}.

error_terminate(Req, State, Class, Reason, Callback, Arity) ->
  error_terminate(Req, State, Class, Reason, Callback, Arity, 500).
error_terminate(Req, State = #state{handler = Handler, handler_state = HandlerState},
		Class, Reason, Callback, Arity, Status) ->
  proxy_terminate(Req, State),
  cowboy_req:maybe_reply(Status, Req),
  erlang:Class([
    {reason, Reason},
    {mfa, {Handler, Callback, Arity}},
    {stacktrace, erlang:get_stacktrace()},
    {req, cowboy_req:to_list(Req)},
    {state, HandlerState}
  ]).

proxy_terminate(Req, #state{handler = Handler, handler_state = HandlerState}) ->
  case erlang:function_exported(Handler, proxy_terminate, 2) of
    true ->
      ok = Handler:proxy_terminate(cowboy_req:lock(Req), HandlerState);
    false ->
      ok
  end.
