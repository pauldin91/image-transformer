defmodule CoreWeb.DownloadController do
  use CoreWeb, :controller
  alias Core.Storage
  alias Core.Uploads

  def download(conn, %{"id" => id}) do

    with fetch_batch(id, conn.assigns.current_user.id),
         {:ok, zip_path} <- Storage.download_batch(id) do
      conn
      |> put_resp_header("content-disposition", ~s(attachment; filename="#{id}.zip"))
      |> send_file(200, List.to_string(zip_path))
      |> halt()
    else
      {:error, :resource_access_denied} ->
        conn
        |> put_status(:unathorized)
        |> text("Insufficient permissions for batch with id #{id}")
        |> halt()

      {:error, :batch_not_found} ->
        conn
        |> put_status(:not_found)
        |> text("Batch with id #{id} does not exist")
        |> halt()

      {:error, :not_found} ->
        conn
        |> put_status(:not_found)
        |> text("File not found")
        |> halt()
    end
  end

  defp fetch_batch(id, user_id) do
    try do
      batch = Uploads.get_batch!(id)

      cond do
        user_id == batch.user_id -> {:ok, batch}
        true -> {:error, :resource_access_denied}
      end
    rescue
      Ecto.NoResultsError -> {:error, :batch_not_found}
    end
  end
end
