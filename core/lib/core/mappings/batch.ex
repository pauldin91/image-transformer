defmodule Core.Mappings.Batch do
  @derive Jason.Encoder
  defstruct [:id,:user_id, :files, :timestamp, :status, :transform]
end
