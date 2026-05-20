defmodule Core.Mappings.Batch do
  @derive Jason.Encoder
  defstruct [:id, :user_id, :files, :timestamp, :status, :transform]

  def from_msg(msg) do
    %Core.Mappings.Batch{
      id: msg["id"],
      user_id: msg["user_id"],
      files: msg["files"],
      timestamp: msg["timestamp"],
      transform: %{
        name: msg["transform"]["name"],
        props: msg["transform"]["props"],
      },
      status: msg["status"]
    }
  end
end
