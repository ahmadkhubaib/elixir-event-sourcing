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

  # private functions

  defp to_event(%{
         type: :aircraft_identified,
         message:
           %{
             icao_address: _icao_address,
             callsign: _callsign,
             emitter_category: _emitter_category
           } = message
       }) do
    new_cloud_event("aircraft_identified", message)
  end

  defp to_event(%{
         type: :squawk_recieved,
         message: %{icao_address: _icao_address, squawk: _squawk} = message
       }) do
    new_cloud_event("squawk_recieved", message)
  end

  defp to_event(%{
         type: :position_reported,
         message: %{
           icao_address: icao_address,
           position: %{altitude: altitude, latitude: latitude, longitude: longitude}
         }
       }) do
    new_cloud_event("position_reported", %{
      icao_address: icao_address,
      latitude: latitude,
      longitude: longitude,
      altitude: altitude
    })
  end

  defp to_event(%{
         type: :velocity_reported,
         message:
           %{
             heading: _heading,
             groud_speed: _ground_speed,
             vertical_rate: _vertical_rate,
             vertical_rate_source: vertical_rate_source
           } = message
       }) do
    source =
      case vertical_rate_source do
        :barometric_pressure -> "barometric_pressure"
        :geometric -> "geometric_pressure"
        _ -> "unknown"
      end

    new_cloud_event("velocity_reported", %{message | vertical_rate_source: source})
  end

  defp to_event(message) do
    Logger.error("unknown message: #{inspect(message)}")
    %{}
  end

  defp new_cloud_event(type, data) do
    %{
      "specversion" => "1.0",
      "type" => "flight_tracker.#{type}",
      "source" => "radio_aggregator",
      "id" => UUID.uuid4(),
      "time" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "datacontenttype" => "application/json",
      "data" => data
    }
    |> Cloudevents.from_map!()
    |> Cloudevents.to_json()
  end
end
