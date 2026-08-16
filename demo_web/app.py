import time
import json
from flask import Flask, request, render_template, Response

app = Flask(__name__)

# Variable global para guardar la última alerta
last_alert = None

@app.route('/')
def index():
    """Renderiza la pantalla del Dashboard."""
    return render_template('index.html')

@app.route('/webhook', methods=['POST'])
def webhook():
    """Endpoint que recibe la petición de RSUDP."""
    global last_alert
    try:
        data = request.json
        print("Alerta recibida:", data)
        # Guardamos la alerta para que el frontend la lea
        last_alert = {
            "timestamp": time.time(),
            "data": data
        }
        return {"status": "success", "message": "Alerta recibida"}, 200
    except Exception as e:
        print("Error procesando webhook:", e)
        return {"status": "error", "message": str(e)}, 400

@app.route('/stream')
def stream():
    """Server-Sent Events (SSE) para enviar la alerta al frontend en tiempo real."""
    def event_stream():
        global last_alert
        reported_alert_time = None
        
        while True:
            # Si hay una nueva alerta que no hemos reportado a este cliente
            if last_alert and last_alert["timestamp"] != reported_alert_time:
                reported_alert_time = last_alert["timestamp"]
                # Enviamos el evento JSON
                yield f"data: {json.dumps(last_alert['data'])}\n\n"
            
            # Pequeña pausa para no saturar la CPU
            time.sleep(0.5)

    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    # Escucha en todas las interfaces para permitir acceso desde otros dispositivos si es necesario
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
