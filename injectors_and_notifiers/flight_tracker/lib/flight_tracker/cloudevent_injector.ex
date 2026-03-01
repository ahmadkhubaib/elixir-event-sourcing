defmodule FlightTracker.CloudeventInjector do
  alias FlightTracker.MessageBroadcaster
  use GenServer
  require Logger

  # Client API
  def start_link(file) do
    GenServer.start_link(__MODULE__, file, name: __MODULE__)
  end

  # Callbacks

  @impl true
  def init(file) do
    Process.send_after(self(), :read_file, 2000)
    {:ok, file}
  end

  @impl true
  def handle_info(:read_file, file) do
    Logger.info("Reading file #{file}")

    file
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Enum.each(fn event -> MessageBroadcaster.broadcast_event(event) end)

    {:noreply, file}
  end
end
