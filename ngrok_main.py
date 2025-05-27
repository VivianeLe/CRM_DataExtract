from pyngrok import ngrok
import streamlit.web.bootstrap

public_url = ngrok.connect(8501)
print(f"🌍 Public URL: {public_url}")

app_file = "app.py"

flag_options = {"server.headless": True}

streamlit.web.bootstrap.run(app_file, command_line="", args=[], flag_options=flag_options)
