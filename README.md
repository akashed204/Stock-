# IB Scanner Dashboard

Streamlit dashboard for scanning NSE symbols and classifying Initial Balance (IB) against ATR using the Alice Blue API.

## Run locally

```bash
pip install -r requirements.txt
streamlit run ib_dashboard.py
```

Open `http://localhost:8501`.

## Deploy on Streamlit Community Cloud

1. Push this project to GitHub.
2. In Streamlit Cloud, create a new app and set main file path to `ib_dashboard.py`.
3. Add app secrets:

```toml
ALICE_USERNAME = "your_alice_username"
ALICE_API_KEY = "your_alice_api_key"
```

You can copy from `.streamlit/secrets.toml.example`.
