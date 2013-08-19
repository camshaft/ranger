-module(ranger_proxy).


-export([init/3]).
-export([handle/2]).
-export([info/3]).
-export([terminate/3]).

-record(state, {
  conn,
  client,
  host,
  port,
  prev
}).

-define(CONNECT_TIMEOUT, 5000).

init({tcp, http}, Req, ProxyOpts) ->
  Host = fast_key:get(host, ProxyOpts, "localhost"),
  Port = fast_key:get(port, ProxyOpts, 5000),

  Opts = [binary, {packet, http_bin}, {packet_size, 1024 * 1024}, {recbuf, 1024 * 1024}, {active, once}, {reuseaddr, true}],

  {ok, Conn} = gen_tcp:connect(Host, Port, Opts, ?CONNECT_TIMEOUT),
  {ok, Req, #state{conn = Conn, host = Host, port = Port}}.

handle(Req, #state{conn = Conn, host = Host, port = Port} = State) ->
  Method = cowboy_req:get(method, Req),
  Path = cowboy_req:get(path, Req),
  Version = cowboy_req:get(version, Req),

  %% TODO rewrite the path based on where we're mounted


  %% Initialize the request by sending the first bytes
  ok = gen_tcp:send(Conn, format_request(Method, Path, Version)),

  Headers = cowboy_req:get(headers, Req),

  %% TODO make the host/port formatting a little smarter
  FixedHeaders = fast_key:set(<<"host">>, <<(list_to_binary(Host))/binary, ":", (integer_to_binary(Port))/binary>>, Headers),

  %% TODO as the outgoing transform to manipulate the outgoing headers
  % {ok, FilteredHeaders, Req2} = filter_headers(Headers, Req)
  % {halt, Req2} = filter_headers(Headers, Req)
  FilteredHeaders = FixedHeaders,

  FormattedHeaders = [format_header(Name, Value) || {Name, Value} <- FilteredHeaders],
  ok = gen_tcp:send(Conn, FormattedHeaders),

  Client = cowboy_req:get(socket, Req),

  inet:setopts(Client, [{packet, raw}, {active, once}]),
  inet:setopts(Conn, [{active, once}]),

  {ok, Req, State#state{client = Client}}.

info({tcp, Client, Data}, Req, State = #state{client = Client, conn = Conn}) ->
  ok = gen_tcp:send(Conn, Data),
  {loop, Req, State#state{prev = send_req_data}, hibernate};
info({tcp, Conn, Data}, Req, State = #state{client = Client, conn = Conn}) ->
  ok = gen_tcp:send(Client, Data),
  {loop, Req, State#state{prev = send_resp_data}, hibernate};
info(Info, Req, State) ->
  io:format("Unhandled message: ~p~n", [Info]),
  {loop, Req, State, hibernate}.

terminate(_Reason, _Req, _State) ->
  ok.

% internal
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
