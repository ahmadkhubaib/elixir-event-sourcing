defmodule FlightTracker.AircraftProjector do
  alias FlightTracker.MessageBroadcaster

  use GenStage
  require Logger

  # Client Api

  def start_link(_) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def get_state_by_icao(icao_address) do
    :ets.lookup(:aircraft_table, icao_address)
    |> case do
      [{_icao, state}] ->
        state

      [] ->
        %{icao_address: icao_address}
    end
  end

  def get_aircraft_by_callsign(callsign) do
    :ets.select(:aircraft_table, [
      {
        {:"$1", :"$2"},
        [
          {:==, {:map_get, :callsign, :"$2"}, callsign}
        ],
        [:"$2"]
      }
    ])
    |> List.first()
  end

  # Callbacks

  @impl true
  def init(:ok) do
    :ets.new(:aircraft_table, [:named_table, :set, :public])
    {:consumer, :ok, subscribe_to: [MessageBroadcaster]}
  end

  @impl true
  def handle_events(events, _from, state) do
    for event <- events do
      handle_event(Cloudevents.from_json!(event))
    end

    {:noreply, [], state}
  end

  # private functions

  defp handle_event(%Cloudevents.Format.V_1_0.Event{
         type: "flight_tracker.aircraft_identified",
         data: data
       }) do
    previous_state = get_state_by_icao(data["icao_address"])

    :ets.insert(
      :aircraft_table,
      {data["icao_address"], Map.put(previous_state, :callsign, data["callsign"])}
    )
  end

  defp handle_event(%Cloudevents.Format.V_1_0.Event{
         type: "flight_tracker.velocity_reported",
         data: data
       }) do
    previous_state = get_state_by_icao(data["icao_address"])

    new_state =
      previous_state
      |> Map.put(:heading, data["heading"])
      |> Map.put(:ground_speed, data["ground_speed"])
      |> Map.put(:vertical_rate, data["vertical_rate"])

    :ets.insert(
      :aircraft_table,
      {data["icao_address"], new_state}
    )
  end

  defp handle_event(%Cloudevents.Format.V_1_0.Event{
         type: "flight_tracker.position_reported",
         data: data
       }) do
    previous_state = get_state_by_icao(data["icao_address"])

    new_state =
      previous_state
      |> Map.put(:latitude, data["latitude"])
      |> Map.put(:longitude, data["longitude"])
      |> Map.put(:altitude, data["altitude"])

    :ets.insert(
      :aircraft_table,
      {data["icao_address"], new_state}
    )
  end

  defp handle_event(_event) do
    Logger.warning("unkown event, ignoring")
  end
end
