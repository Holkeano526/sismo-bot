import time
import requests
from datetime import datetime

# URL del feed de terremotos del USGS (Todos los sismos de la última hora)
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
# URL de tu Dashboard Web Local
WEBHOOK_URL = "http://localhost:5000/webhook"
# Magnitud mínima para disparar la alarma
MIN_MAGNITUDE = 2.0

# Set para recordar los IDs de sismos que ya hemos notificado
processed_ids = set()
first_run = True

print("Iniciando Vigilante USGS...")
print(f"Buscando sismos con magnitud >= {MIN_MAGNITUDE}")

def send_alert(feature):
    props = feature['properties']
    mag = props['mag']
    place = props['place']
    # El tiempo en USGS está en milisegundos
    event_time = datetime.fromtimestamp(props['time'] / 1000.0)
    
    timestamp_str = event_time.strftime("%Y-%m-%d %H:%M:%S")
    
    payload = {
        "timestamp": timestamp_str,
        "station": "USGS Global Feed",
        "mensaje": f"Magnitud: {mag} - Lugar: {place}",
        "mag": mag,
        "place": place,
        "is_real_data": True
    }
    
    try:
        response = requests.post(WEBHOOK_URL, json=payload, timeout=5)
        response.raise_for_status()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Alerta enviada al Dashboard: {mag} en {place}")
    except requests.exceptions.RequestException as e:
        print(f"Error al enviar la alerta: {e}")

while True:
    try:
        response = requests.get(USGS_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Enviar un "Latido" (Heartbeat) a la interfaz para que el usuario sepa que estamos escaneando
        try:
            requests.post(WEBHOOK_URL, json={"type": "heartbeat", "time": datetime.now().strftime('%H:%M:%S')}, timeout=2)
        except:
            pass # Ignoramos si el heartbeat falla
            
        # USGS devuelve los sismos ordenados por el más reciente
        features = data.get('features', [])
        
        if first_run and features:
            # En la primera ejecución, mandamos el sismo más reciente para probar que funciona
            latest = features[0]
            processed_ids.add(latest['id'])
            print("--- PRIMERA EJECUCIÓN: Enviando el sismo más reciente para demostración ---")
            send_alert(latest)
            first_run = False
            
            # Registrar el resto de los sismos actuales para no enviar alertas repetidas
            for feature in features:
                processed_ids.add(feature['id'])
        else:
            # En ejecuciones subsecuentes, buscamos nuevos sismos
            for feature in features:
                eq_id = feature['id']
                mag = feature['properties']['mag']
                
                # A veces la magnitud puede ser null en eventos muy nuevos
                if mag is None:
                    continue
                    
                if eq_id not in processed_ids and mag >= MIN_MAGNITUDE:
                    print("¡Nuevo sismo detectado!")
                    send_alert(feature)
                    processed_ids.add(eq_id)
                elif eq_id not in processed_ids:
                    # Lo marcamos como procesado aunque no haya alcanzado la magnitud
                    processed_ids.add(eq_id)
                    
    except Exception as e:
        print(f"Error al consultar el USGS: {e}")
        
    # Esperamos 5 segundos antes de volver a consultar
    time.sleep(5)
