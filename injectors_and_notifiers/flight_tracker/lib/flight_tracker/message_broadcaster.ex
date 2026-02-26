defmodule FlightTracker.MessageBroadcaster do
  use GenStage
  require Logger

  # Client Api

  def start_link(_) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  inject raw message which is not in cloud event format
  """

  def broadcast_message(message) do
    GenStage.call(__MODULE__, {:notify_message, message})
  end

  @doc """
  inject a cloud event to be published to stage pipeline
  """

  def broadcast_event(event) do
    GenStage.call(__MODULE__, {:notify_event, event})
  end


  # Callbacks
  @impl true
  def init(:ok) do
    {:producer, :ok, dispatcher: GenStage.BroadcastDispatcher}
  end

  @impl true
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_call({:notify_message, message}, _from, state) do
    {:reply, :ok, [to_event(message)], state}
  end

  @impl true
  def handle_call({:notify_event, event}, _from, state) do
    {:reply, :ok, [event], state}
  end
end
