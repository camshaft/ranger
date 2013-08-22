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
  req_headers :: binary()
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
  try
    [Method, Headers] = cowboy_req:get([method, headers], Req),
    case erlang:function_exported(Handler, proxy_init, 2) of
      true ->
        try Handler:proxy_init(Req, HandlerOpts) of
          {ok, Req2, HandlerState} ->
            backend(Req2, #state{env=Env, method=Method, req_headers=Headers,
              handler=Handler, handler_state=HandlerState})
        catch Class:Reason ->
          error_logger:error_msg(
            "** Cowboy handler ~p terminating in ~p/~p~n"
            "   for the reason ~p:~p~n** Options were ~p~n"
            "** Request was ~p~n** Stacktrace: ~p~n~n",
            [Handler, proxy_init, 2, Class, Reason, HandlerOpts,
              cowboy_req:to_list(Req), erlang:get_stacktrace()]),
          {error, 500, Req}
        end;
      false ->
        backend(Req, #state{env=Env, method=Method,
          handler=Handler})
    end
  catch
    throw:{?MODULE, error} ->
      {error, 500, Req}
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
      next(Req, State#state{timeout=?DEFAULT_TIMEOUT}, fun open_connection/2);
    {Timeout, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           timeout=Timeout},
      next(Req2, State2, fun open_connection/2)
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

init_request(Req, State=#state{conn=Conn, method=Method}) ->
  {Path, Req2} = cowboy_req:path_info(Req),
  {Version, Req3} = cowboy_req:version(Req2),
  Message = format_request(Method, Path, Version),
  case gen_tcp:send(Conn, Message) of
    ok ->
      next(Req3, State, fun forwarded_header_prefix/2);
    {error, _Error} ->
      next(Req3, State, 502)
  end.

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

request_id(Req, State=#state{req_headers=ReqHeaders}) ->
  case call(Req, State, request_id) of
    no_call ->
      next(Req, State, fun req_headers/2);
    {ID, Req2, HandlerState} ->
      Req3 = cowboy_req:set_resp_header(<<"x-request-id">>, ID, Req2),
      State2 = State#state{handler_state=HandlerState,
                           req_headers=[{<<"x-request-id">>, ID}|ReqHeaders]},
      next(Req3, State2, fun req_headers/2)
  end.

req_headers(Req, State=#state{req_headers=ReqHeaders}) ->
  case call(Req, State, req_headers, ReqHeaders) of
    no_call ->
      next(Req, State, fun send_req_headers/2);
    {ReqHeaders2, Req2, HandlerState} ->
      State2 = State#state{handler_state=HandlerState,
                           req_headers=ReqHeaders2},
      next(Req2, State2, fun send_req_headers/2)
  end.

send_req_headers(Req, State=#state{conn=Conn, req_headers=ReqHeaders, backend=Backend}) ->
  FormattedHeaders = [case Name of
    <<"host">> ->
      format_header(Name, format_host_header(Backend));
    _ ->
      format_header(Name, Value)
  end || {Name, Value} <- ReqHeaders],

  case gen_tcp:send(Conn, [FormattedHeaders, <<"\r\n">>]) of
    ok ->
      next(Req, State, fun req_body/2);
    {error, _Error} ->
      next(Req, State, 502)
  end.

req_body(Req, State) ->
  respond(Req, State, 200).

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

%% Protocol

call(Req, State=#state{handler=Handler, handler_state=HandlerState}, Callback) ->
  case erlang:function_exported(Handler, Callback, 2) of
    true ->
      try
        Handler:Callback(Req, HandlerState)
      catch Class:Reason ->
        error_logger:error_msg(
          "** Cowboy handler ~p terminating in ~p/~p~n"
          "   for the reason ~p:~p~n** Handler state was ~p~n"
          "** Request was ~p~n** Stacktrace: ~p~n~n",
          [Handler, Callback, 2, Class, Reason, HandlerState,
            cowboy_req:to_list(Req), erlang:get_stacktrace()]),
        error_terminate(Req, State)
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
        error_logger:error_msg(
          "** Cowboy handler ~p terminating in ~p/~p~n"
          "   for the reason ~p:~p~n** Handler state was ~p~n"
          "** Request was ~p~n** Stacktrace: ~p~n~n",
          [Handler, Callback, 2, Class, Reason, HandlerState,
            cowboy_req:to_list(Req), erlang:get_stacktrace()]),
        error_terminate(Req, State)
      end;
    false ->
      no_call
  end.

next(Req, State, Next) when is_function(Next) ->
  Next(Req, State);
next(Req, State, StatusCode) when is_integer(StatusCode) ->
  respond(Req, State, StatusCode).

respond(Req, State, StatusCode) ->
  {ok, Req2} = cowboy_req:reply(StatusCode, Req),
  terminate(Req2, State).

terminate(Req, State=#state{env=Env}) ->
  proxy_terminate(Req, State),
  {ok, Req, [{result, ok}|Env]}.

-spec error_terminate(cowboy_req:req(), #state{}) -> no_return().
error_terminate(Req, State) ->
  proxy_terminate(Req, State),
  erlang:raise(throw, {?MODULE, error}, erlang:get_stacktrace()).

proxy_terminate(Req, #state{handler=Handler, handler_state=HandlerState}) ->
  case erlang:function_exported(Handler, proxy_terminate, 2) of
    true -> ok = Handler:proxy_terminate(
      cowboy_req:lock(Req), HandlerState);
    false -> ok
  end.
