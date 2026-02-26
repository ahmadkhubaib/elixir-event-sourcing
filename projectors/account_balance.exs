defmodule Projectors.AccountBalance do
  @doc """
  start registry and apply events after that

  iex(1)> {:ok, _} = Registry.start_link(keys: :unique, name: Registry.AccountProjectors)
  {:ok, #PID<0.111.0>}
  iex(2)> Projectors.AccountBalance.apply_event(%{account_number: "newacc", value: 100, event_type: :amount_deposited})
  iex(3)> Projectors.AccountBalance.apply_event(%{account_number: "newacc", value: 100, event_type: :amount_deposited})
  :ok
  iex(4)> Projectors.AccountBalance.apply_event(%{account_number: "newacc", value: 100, event_type: :amount_deposited})
  :ok
  iex(5)> Projectors.AccountBalance.balance_lookup "myacc"
  {:error, :unknown_account}
  iex(6)> Projectors.AccountBalance.balance_lookup "newacc"
  {:ok, 300}
  iex(7)> Projectors.AccountBalance.apply_event(%{account_number: "newacc", value: 100, event_type: :fee_applied})
  :ok
  iex(8)> Projectors.AccountBalance.balance_lookup "newacc"
  {:ok, 200}
  """

  use GenServer
  require Logger

  def start_link(account_number) do
    GenServer.start_link(
      __MODULE__,
      account_number,
      name: via(account_number)
    )
  end

  def apply_event(%{account_number: account_number} = event) when is_binary(account_number) do
    case Registry.lookup(Registry.AccountProjectors, account_number) do
      [{pid, _}] ->
        apply_event(pid, event)

      _ ->
        Logger.debug("attempt to apply event to non existant acc")
        {:ok, pid} = start_link(account_number)
        apply_event(pid, event)
    end
  end

  def apply_event(pid, event) when is_pid(pid) do
    GenServer.cast(pid, {:handle_event, event})
  end

  def handle_event(
        %{event_type: :amount_deposited, value: value} = _event,
        %{balance: balance} = state
      ) do
    %{state | balance: balance + value}
  end

  def handle_event(
        %{event_type: :amount_withdrawn, value: value} = _event,
        %{balance: balance} = state
      ) do
    %{state | balance: balance - value}
  end

  def handle_event(
        %{event_type: :fee_applied, value: value} = _event,
        %{balance: balance} = state
      ) do
    %{state | balance: balance - value}
  end

  def balance_lookup(account_number) when is_binary(account_number) do
    case Registry.lookup(Registry.AccountProjectors, account_number) do
      [{pid, _}] ->
        {:ok, get_balance(pid)}

      _ ->
        {:error, :unknown_account}
    end
  end

  def get_balance(pid) do
    GenServer.call(pid, :get_balance)
  end

  @impl true
  def init(account_number) do
    {:ok, %{account_number: account_number, balance: 0}}
  end

  @impl true
  def handle_cast({:handle_event, event}, state) do
    {:noreply, handle_event(event, state)}
  end

  @impl true
  def handle_call(:get_balance, _from, state) do
    {:reply, state.balance, state}
  end

  defp via(account_number) do
    {:via, Registry, {Registry.AccountProjectors, account_number}}
  end
end
