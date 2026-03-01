defmodule FlightTracker.FlightNotifier do
  alias FlightTracker.MessageBroadcaster
  alias FlightTracker.AircraftProjector

  use GenStage
  require Logger

  # Client API
  def start_link(flight_call_sign) do
    GenStage.start_link(__MODULE__, flight_call_sign, name: __MODULE__)
  end

  # Callbacks
  @impl true
  def init(flight_call_sign) do
    {:consumer, flight_call_sign, subscribe_to: [MessageBroadcaster]}
  end

  @impl true
  def handle_events(events, _from, state) do
    for event <- events do
      handle_event(Cloudevents.from_json!(event), state)
    end

    {:noreply, [], state}
  end

  # private functions
  defp handle_event(
         %Cloudevents.Format.V_1_0.Event{
           type: "flight_tracker.position_reported",
           data: data,
           time: time
         },
         flight_call_sign
       ) do
    aircraft = AircraftProjector.get_state_by_icao(data["icao_address"])

    if String.trim(Map.get(aircraft, :callsign, "")) == flight_call_sign do
      Logger.info(
        "Flight #{flight_call_sign} has reported position, lat #{data["latitude"]}, long #{data["longitude"]} at #{time}"
      )
    end
  end

  defp handle_event(
         %Cloudevents.Format.V_1_0.Event{
           type: "flight_tracker.aircraft_identified",
           data: data,
            time: time
         },
         flight_call_sign
       ) do
    if String.trim(data["callsign"]) == flight_call_sign do
      Logger.info("Flight #{flight_call_sign} has been identified at #{time}")
    end
  end

  defp handle_event(
         %Cloudevents.Format.V_1_0.Event{
           type: "flight_tracker.velocity_reported",
           data: data,
           time: time
         },
         flight_call_sign
       ) do
    aircraft = AircraftProjector.get_state_by_icao(data["icao_address"])

    if String.trim(Map.get(aircraft, :callsign, "")) == flight_call_sign do
      Logger.info(
        "Flight #{flight_call_sign} has reported velocity, vertical speed #{data["vertical_speed"]}, ground speed #{data["ground_speed"]} and heading #{data["heading"]} at #{time}"
      )
    end
  end

  defp handle_event(_event, _flight_call_sign) do
  end
end
