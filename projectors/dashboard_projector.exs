defmodule Projectors.LeaderBoard do
  use GenServer
  require Logger

  # Client Api

  def start_link() do
    GenServer.start_link(__MODULE__, nil)
  end

  def apply_event(pid, event) do
    GenServer.cast(pid, {:handle_event, event})
  end

  def get_top_10(pid) do
    GenServer.call(pid, :get_top_10)
  end

  def get_score(pid, attacker) do
    GenServer.call(pid, {:get_score, attacker})
  end

  # Callbacks

  @impl true
  def init(_) do
    {:ok, %{scores: 0, top_10: []}}
  end

  @impl true
  def handle_cast({:handle_event, %{event_type: zombie_killed, attacker: attacker}}, state) do
    new_score = Map.update(state.scores, attacker, 1, &(&1 + 1))
    {:noreply, %{state | scores: new_score, top_10: rerank(new_score)}}
  end

  @impl true
  def handle_cast({:handle_event, %{event_type: :week_completed}}, _state) do
    {:noreply, %{scores: 0, top_10: []}}
  end

  @impl true
  def handle_call(:get_top_10, _from, state) do
    {:reply, state.top_10, state}
  end

  @impl true
  def handle_call({:get_score, attacker}, _from, state) do
    {:reply, Map.get(state.scores, attacker, 0), state}
  end

  # private functions

  defp rerank(scores) when is_map(scores) do
    scores
    |> Map.to_list()
    |> Enum.sort(fn {_k1, v1}, {_k2, v2} -> v1 >= v2 end)
    Enum.take(10)
  end
end
