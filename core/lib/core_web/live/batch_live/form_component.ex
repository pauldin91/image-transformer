defmodule CoreWeb.BatchLive.FormComponent do
  alias Core.Validators
  use CoreWeb, :live_component

  alias Core.Uploads
  alias Core.Uploads.Formatter
  alias Core.Handlers
  alias Core.Storage
  alias Core.Transforms

  @impl true
  def update(%{batch: batch} = assigns, socket) do
    {:ok,
     socket
     |> assign(assigns)
     |> assign_new(:transform, fn -> "none" end)
     |> assign_new(:props_entries, fn -> [] end)
     |> assign(:transformations, Transforms.list_transforms())
     |> assign_new(:form, fn ->
       to_form(Uploads.change_batch(batch))
     end)
     |> allow_upload(:files,
       accept: :any,
       max_entries: 10,
       max_file_size: 50_000_000
     )}
  end

  @impl true
  def handle_event("validate", params, socket) do
    batch_params = params["batch"] || %{}
    props_params = params["props"] || %{}

    transform = batch_params["transform"] || socket.assigns.transform

    props_entries =
      transform
      |> Transforms.build_props_for_transform()
      |> merge_props_values(props_params)

    changeset =
      socket.assigns.batch
      |> Uploads.change_batch(batch_params)
      |> Map.put(:action, :validate)

    {:noreply,
     socket
     |> assign(:form, to_form(changeset))
     |> assign(:transform, transform)
     |> assign(:props_entries, props_entries)}
  end

  def handle_event("toggle_transform", %{"mode" => mode}, socket) do
    {:noreply, assign(socket, :mode, mode)}
  end

  def handle_event("add_prop", _params, socket) do
    entry = %{
      id: Ecto.UUID.generate(),
      key: "",
      value: ""
    }

    {:noreply,
     update(socket, :props_entries, fn entries ->
       entries ++ [entry]
     end)}
  end

  def handle_event("remove_prop", %{"id" => id}, socket) do
    {:noreply,
     update(socket, :props_entries, fn entries ->
       Enum.reject(entries, &(&1.id == id))
     end)}
  end

  @impl true
  def handle_event("save", _params, %{assigns: %{user: user}} = socket) do
    uuid = Ecto.UUID.generate()

    props =
      socket.assigns.props_entries
      |> Map.new(fn %{key: k, value: v} -> {k, to_string(v)} end)

    case Validators.Transform.validate(props, socket.assigns.transform) do
      {:ok, _spec} ->
        result =
          Handlers.handle_upload(%Core.Mappings.Batch{
            id: uuid,
            user_id: user.id,
            files: copy_files(uuid, socket),
            status: "queued",
            timestamp: DateTime.utc_now(),
            transform: %{
              name: socket.assigns.transform,
              props: props
            }
          })

        case result do
          {:ok, batch_dto} ->
            {:noreply,
             socket
             |> assign(:batch_id, batch_dto.id)
             |> put_flash(:info, "Files uploaded with batch id #{batch_dto.id}")}

          {:error, reason} ->
            {:noreply, put_flash(socket, :error, reason)}

          :error ->
            {:noreply, put_flash(socket, :error, "Incognito error")}
        end

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, reason)}
    end
  end

  defp copy_files(uuid, socket) do
    consume_uploaded_entries(socket, :files, fn %{path: path}, entry ->
      Storage.store_entry(%Core.Mappings.Entry{
        path: path,
        filename: entry.client_name,
        content_type: entry.client_type,
        batch_id: uuid
      })
    end)
  end

  defp merge_props_values(entries, props_params) do
    Enum.map(entries, fn entry ->
      case Map.fetch(props_params, entry.key) do
        {:ok, value} ->
          %{entry | value: value}

        :error ->
          entry
      end
    end)
  end
end
