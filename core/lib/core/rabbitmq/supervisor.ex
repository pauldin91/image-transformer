defmodule Core.RabbitMq.Supervisor do
  use Supervisor

  def start_link(_args) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      %{
        id: Core.RabbitMq.Publisher,
        start:
          {Core.RabbitMq.Publisher, :start_link,
           [
             [
               name: Core.RabbitMq.Publisher
             ]
           ]},
        restart: :permanent,
        shutdown: 10_000
      },
      %{
        id: Core.RabbitMq.Consumer,
        start:
          {Core.RabbitMq.Consumer, :start_link,
           [
             [
               queue: Application.fetch_env!(:core, :processed_queue),
               name: Core.RabbitMq.Consumer
             ]
           ]},
        restart: :permanent,
        shutdown: 10_000
      },
      %{
        id: Core.RabbitMq.Ingest,
        start:
          {Core.RabbitMq.Ingest, :start_link,
           [
             [
               queue: Application.fetch_env!(:core, :ingest_queue),
               name: Core.RabbitMq.Ingest
             ]
           ]},
        restart: :permanent,
        shutdown: 10_000
      }
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
