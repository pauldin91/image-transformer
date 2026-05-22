defmodule Core.Handlers do
  alias Core.Metadata
  alias Core.Uploads
  alias Core.Storage
  alias Core.Items


  def ingest_publish(%Core.Mappings.Batch{} = dto) do
    with {:ok, result} <- Jason.encode(dto),
         :ok <-
           get_event_queue(:ingest_queue)
           |> Core.RabbitMq.Publisher.publish_message(result) do
      {:ok, dto}
    end
  end

  def create_batch(%Core.Mappings.Batch{} = batch_dto) do

    status = "Processing"

    with {:ok, batch} <-
           Uploads.create_batch(%{
             id: batch_dto.id,
             status: status,
             transform: batch_dto.transform.name,
             user_id: batch_dto.user_id,
             inserted_at: batch_dto.timestamp
           }),
         :ok <- link_all_pictures(batch_dto),
         {:ok, _serialized} <-
           Metadata.save(%Core.Mappings.Batch{
             batch_dto
             | timestamp: batch.inserted_at,
               status: status
           }) do
      {:ok, batch.id}
    end
  end

  defp link_all_pictures(batch_dto) do
    Enum.reduce_while(batch_dto.files, :ok, fn file, :ok ->
      case link_picture(file, batch_dto.id) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp link_picture(stored, batch_id) do
    with {:ok, _picture} <-
           Items.create_picture(%{
            batch_id: batch_id,
             name: stored["filename"],
             size: stored["size"]
           }) do
      :ok
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_event_queue(:ingest_queue), do: Application.fetch_env!(:core,:ingest_queue)
  defp get_event_queue("convert"),
    do: Application.fetch_env!(:core, :processing_queues) |> Enum.at(0)

  defp get_event_queue(_name), do: Application.fetch_env!(:core, :processing_queues) |> Enum.at(1)

  def purge_user_batches(user_id) do
    Uploads.list_batch_ids_of_user(user_id)
    |> Storage.purge_uploads_with_ids()

    Uploads.delete_batches_of_user(user_id)
  end
end
