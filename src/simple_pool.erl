-module(simple_pool).
-author('mathieu@garambrogne.net').

-behaviour(gen_server).

%% gen_server callbacks
-export([start_link/0, init/1, handle_call/3, handle_cast/2, 
handle_info/2, terminate/2, code_change/3]).

-export([
    add_worker/2,
    work/2
]).

-record(state, {
    workers,
    queue,
    reserved
    }).
%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

add_worker(Pool, WorkerPid) ->
    gen_server:cast(Pool, {add_worker, WorkerPid}).

work(Pool, Message) ->
    case gen_server:call(Pool, reserve) of
        wait -> 
            receive
                {worker, Worker} ->
                    work_(Pool, Worker, Message)
            end;
        {direct, Worker} ->
            work_(Pool, Worker, Message)
    end.

work_(Pool, Worker, Message) ->
   R = gen_server:call(Worker, Message),
   ok = gen_server:cast(Pool, {release, Worker}),
   R.
 
%%====================================================================
%% api callbacks
%%====================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{
        workers = [],
        reserved = [],
        queue = queue:new()
   }}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(reserve, From, #state{reserved=Reserved, workers=Workers, queue=Queue} = State) ->
    {Type, NewQueue, NewWorkers, NewReserved} = case Workers of
        [] ->
            {wait, queue:in(From, Queue), [], Reserved};
        _  ->
            [Worker|TailWorkers] = Workers,
            {{direct, Worker}, Queue, TailWorkers, [Worker|Reserved]} 
    end,
    {reply, Type, State#state{reserved=NewReserved, workers=NewWorkers, queue=NewQueue}};

handle_call(_Request, _From, State) ->
    {reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({add_worker, WorkerPid}, #state{workers = Workers} = State) ->
    {noreply, State#state{workers = [WorkerPid | Workers]}};

handle_cast({release, WorkerPid}, #state{workers = Workers, queue = Queue, reserved=Reserved} = State) ->
    NewQueue = case queue:len(Queue) of
        0 -> Queue;
        _ ->
            {{value, From}, Queue2} = queue:out(Queue),
            [WorkerPid | NewWorkers] = Workers, 
            From ! {worker, WorkerPid},
            Queue2
    end,
    {noreply, State#state{
        reserved = lists:delete(WorkerPid, Reserved),
        workers = [WorkerPid | Workers],
        queue = NewQueue
    }};
    
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Private API
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
    simple_test() ->
        {ok, Pool} = start_link(),
        {ok, Worker1} = sp_dummy_worker:start_link(),
        {ok, Worker2} = sp_dummy_worker:start_link(),
        add_worker(Pool, Worker1),
        add_worker(Pool, Worker2),
        spawn(fun() -> 42 = work(Pool, wait) end),
        spawn(fun() -> 42 = work(Pool, wait) end),
        spawn(fun() -> 42 = work(Pool, wait) end).
-endif.

