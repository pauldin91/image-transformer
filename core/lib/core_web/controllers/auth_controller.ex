defmodule CoreWeb.AuthController do
  use CoreWeb, :controller
  plug(Ueberauth)

  alias Core.Accounts

  def callback(%{assigns: %{ueberauth_failure: %Ueberauth.Failure{}}} = conn, _params) do
    conn
    |> put_flash(:error, "Failed to authenticate")
    |> redirect(to: ~p"/")
  end

  def callback(%{assigns: %{ueberauth_auth: auth}} = conn, _params) do

    case  Accounts.find_or_create_from_oauth(auth) do
      {:ok,{session_token, user}} ->
        conn
        |> put_session(:user_token, session_token)
        |> put_flash(:info, "Welcome, #{user.email}!")
        |> redirect(to: ~p"/")

      {:error, _reason} ->
        conn
        |> put_flash(:error, "Authentication failed.")
        |> redirect(to: "/login")
    end
  end



  def signout(conn, _params) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: ~p"/")
  end

end
