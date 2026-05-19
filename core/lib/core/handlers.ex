defmodule Core.Handlers do
  alias Core.Metadata
  alias Core.Uploads
  alias Core.Storage
  alias Core.Items

  alias Core.Mappings.Stored

  @spec handle_upload(any(), %{
          :batch_id => any(),
          :files => any(),
          :props => any(),
          :transform => any(),
          optional(any()) => any()
        }) :: {:ok, any()}
  def handle_upload(
        user,
        %{files: files, transform: transform, batch_id: batch_id, props: props}
      ) do
    with :ok <- publish_batch(
      %Core.Mappings.Batch{
        id: batch_id,
        user_id: user.id,
        files: files,
        status: "queued",
        timestamp: DateTime.utc_now,
        transform: %{
          name: transform,
          props: props
        }
    }) do
      {:ok, batch_id}
    end

    # create_batch_with_pictures(
    #   %Core.Mappings.Batch{
    #     id: batch_id,
    #     files: files,
    #     transform: %{
    #       name: transform,
    #       props: props
    #     }
    #   },
    #   %{user_id: user.id}
    # )
  end

  defp create_batch_with_pictures(%Core.Mappings.Batch{} = batch_dto, %{user_id: user_id}) do
    status = "Processing"

    with {:ok, batch} <-
           Uploads.create_batch(%{
             id: batch_dto.id,
             status: status,
             transform: batch_dto.transform.name,
             user_id: user_id,
             inserted_at: batch_dto.timestamp
           }),
         :ok <- link_all_pictures(batch_dto),
         {:ok, _serialized} <-
           Metadata.save(%Core.Mappings.Batch{
             batch_dto
             | timestamp: batch.inserted_at,
               status: status
           }) do
        #  :ok <- publish_batch(serialized, batch_dto.transform.name) do
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

  defp link_picture(%Stored{} = stored, batch_id) do
    with {:ok, _picture} <-
           Items.create_picture(%{
             batch_id: batch_id,
             name: stored.filename,
             size: stored.size
           }) do
      :ok
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp publish_batch(%Core.Mappings.Batch{}=batch) do
    queue =
      cond do
        batch.transform.name == "convert" -> get_event_queue(:none)
        true -> get_event_queue(batch.transform.name)
      end

    Core.RabbitMq.Publisher.publish_message(queue, batch)
  end

  defp get_event_queue(:none), do: Application.fetch_env!(:core, :processing_queues) |> Enum.at(0)
  defp get_event_queue(_name), do: Application.fetch_env!(:core, :processing_queues) |> Enum.at(1)

  def purge_user_batches(user_id) do
    Uploads.list_batch_ids_of_user(user_id)
    |> Storage.purge_uploads_with_ids()

    Uploads.delete_batches_of_user(user_id)
  end
end
