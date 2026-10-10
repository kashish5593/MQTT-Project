import json
import time
import threading
from collections import deque
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objs as go
from paho.mqtt import client as mqtt_client

MAX_POINTS = 60
data_store = deque(maxlen=MAX_POINTS)
latest_payload = {}
connection_state = {"status": "Disconnected", "error": None}

mqtt_instance = None
mqtt_thread = None

def mqtt_worker(broker, port, topic):
    global mqtt_instance, connection_state
    client_id = f"dash-sub-{int(time.time())}"
    
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            connection_state["status"] = f"Connected to {broker}:{port}"
            connection_state["error"] = None
            client.subscribe(topic)
        else:
            connection_state["status"] = "Connection Failed"
            connection_state["error"] = f"Return code: {rc}"

    def on_disconnect(client, userdata, rc, properties=None):
        connection_state["status"] = "Disconnected"

    def on_message(client, userdata, msg):
        global latest_payload
        try:
            payload_str = msg.payload.decode("utf-8")
            data = json.loads(payload_str)
            timestamp = time.strftime("%H:%M:%S")
            data["_timestamp"] = timestamp
            latest_payload = data
            data_store.append(data)
        except Exception:
            try:
                val = float(msg.payload.decode("utf-8"))
                timestamp = time.strftime("%H:%M:%S")
                data = {"reading": val, "_timestamp": timestamp}
                latest_payload = data
                data_store.append(data)
            except Exception:
                pass

    try:
        connection_state["status"] = f"Connecting to {broker}:{port} on '{topic}'..."
        client = mqtt_client.Client(
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
            client_id=client_id
        )
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        
        mqtt_instance = client
        client.connect(broker, int(port), keepalive=60)
        client.loop_forever()
    except Exception as e:
        connection_state["status"] = "Error"
        connection_state["error"] = str(e)

def start_mqtt_client(broker, port, topic):
    global mqtt_thread, mqtt_instance
    if mqtt_instance:
        try:
            mqtt_instance.disconnect()
            mqtt_instance.loop_stop()
        except Exception:
            pass
    mqtt_thread = threading.Thread(target=mqtt_worker, args=(broker, port, topic), daemon=True)
    mqtt_thread.start()

start_mqtt_client("192.168.1.100", 1885, "mqttdata")

app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(style={"fontFamily": "Segoe UI, sans-serif", "padding": "24px", "backgroundColor": "#f4f6f9"}, children=[
    html.H2("⚡ Live MQTT Telemetry Dashboard", style={"color": "#1e293b", "marginTop": "0"}),
    
    html.Div(style={
        "display": "flex", "gap": "15px", "alignItems": "flex-end", "flexWrap": "wrap",
        "backgroundColor": "#fff", "padding": "16px", "borderRadius": "8px",
        "boxShadow": "0 1px 3px rgba(0,0,0,0.1)", "marginBottom": "20px"
    }, children=[
        html.Div([
            html.Label("Broker Host:", style={"fontWeight": "bold", "fontSize": "13px"}),
            dcc.Input(id="broker-ip", type="text", value="192.168.1.100", style={"width": "160px", "padding": "6px", "display": "block"})
        ]),
        html.Div([
            html.Label("Port:", style={"fontWeight": "bold", "fontSize": "13px"}),
            dcc.Input(id="broker-port", type="number", value=1885, style={"width": "90px", "padding": "6px", "display": "block"})
        ]),
        html.Div([
            html.Label("MQTT Topic:", style={"fontWeight": "bold", "fontSize": "13px"}),
            dcc.Input(id="broker-topic", type="text", value="mqttdata", style={"width": "180px", "padding": "6px", "display": "block"})
        ]),
        html.Button("Apply & Connect", id="connect-btn", n_clicks=0, style={
            "backgroundColor": "#2563eb", "color": "#fff", "border": "none", "padding": "8px 18px",
            "borderRadius": "4px", "cursor": "pointer", "fontWeight": "bold", "height": "34px"
        })
    ]),

    html.Div(id="status-container", style={
        "padding": "12px 16px", "borderRadius": "6px", "marginBottom": "20px", "fontWeight": "bold"
    }),

    html.Div(id="metric-cards", style={"display": "flex", "gap": "15px", "flexWrap": "wrap", "marginBottom": "20px"}),

    html.Div(style={"backgroundColor": "#fff", "padding": "16px", "borderRadius": "8px", "boxShadow": "0 1px 3px rgba(0,0,0,0.1)"}, children=[
        dcc.Graph(id="live-telemetry-graph")
    ]),

    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
])

@app.callback(
    Output("status-container", "children"),
    Output("status-container", "style"),
    Input("connect-btn", "n_clicks"),
    Input("interval-component", "n_intervals"),
    State("broker-ip", "value"),
    State("broker-port", "value"),
    State("broker-topic", "value")
)
def handle_connection(n_clicks, n_intervals, host, port, topic):
    ctx = dash.callback_context
    if ctx.triggered and ctx.triggered[0]["prop_id"] == "connect-btn.n_clicks":
        data_store.clear()
        start_mqtt_client(host, port, topic)

    stat = connection_state["status"]
    err = connection_state["error"]
    
    is_connected = "Connected to" in stat
    bg = "#dcfce7" if is_connected else "#fee2e2"
    color = "#166534" if is_connected else "#991b1b"
    msg = f"Status: {stat} | Active Topic: '{topic}'" + (f" | Detail: {err}" if err else "")
    
    return msg, {"backgroundColor": bg, "color": color, "padding": "12px 16px", "borderRadius": "6px", "marginBottom": "20px"}

@app.callback(
    Output("live-telemetry-graph", "figure"),
    Output("metric-cards", "children"),
    Input("interval-component", "n_intervals")
)
def update_dashboard(n):
    if not data_store:
        fig = go.Figure()
        fig.update_layout(title="Waiting for MQTT payload on topic...", template="plotly_white")
        card = html.Div("No messages received yet. Send a payload like: {\"temp\": 28.5, \"humidity\": 62}", 
                        style={"color": "#64748b", "fontStyle": "italic"})
        return fig, card

    all_keys = set()
    for entry in data_store:
        for k, v in entry.items():
            if k != "_timestamp" and isinstance(v, (int, float)):
                all_keys.add(k)

    timestamps = [entry["_timestamp"] for entry in data_store]
    fig = go.Figure()

    for key in sorted(all_keys):
        values = [entry.get(key, None) for entry in data_store]
        fig.add_trace(go.Scatter(x=timestamps, y=values, mode="lines+markers", name=key.capitalize()))

    fig.update_layout(
        title="Live Sensor Readings",
        xaxis_title="Time",
        yaxis_title="Value",
        margin=dict(l=40, r=40, t=50, b=40),
        template="plotly_white",
        hovermode="x unified"
    )

    cards = []
    for k, v in latest_payload.items():
        if k == "_timestamp":
            continue
        cards.append(html.Div(style={
            "backgroundColor": "#fff", "padding": "14px 20px", "borderRadius": "8px",
            "boxShadow": "0 1px 3px rgba(0,0,0,0.1)", "minWidth": "140px"
        }, children=[
            html.Div(k.upper(), style={"fontSize": "11px", "color": "#64748b", "fontWeight": "bold"}),
            html.Div(str(v), style={"fontSize": "22px", "color": "#0f172a", "fontWeight": "bold", "marginTop": "4px"})
        ]))

    return fig, cards

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)