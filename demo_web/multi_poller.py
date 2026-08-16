import asyncio
import aiohttp
import websockets
import feedparser
import json
import math
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Set

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("MultiPollerWeb")

# Configuración
MIN_MAGNITUDE: float = float(os.environ.get("MIN_MAGNITUDE", "1.0")) # Menor para que detecte micro-sismos
POLL_INTERVAL: int = int(os.environ.get("POLL_INTERVAL", "30"))
WEBHOOK_URL: str = "http://localhost:5000/webhook"

# Fuentes de Datos
USGS_URL: str = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
EMSC_WS_URL: str = "wss://www.seismicportal.eu/standing_order/websocket"
GFZ_RSS_URL: str = "https://geofon.gfz-potsdam.de/eqinfo/list.php?fmt=rss"

def is_in_south_america(lat: float, lon: float) -> bool:
    """Verifica si las coordenadas están aproximadamente dentro de Sudamérica."""
    return -60.0 <= lat <= 15.0 and -85.0 <= lon <= -30.0

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia en kilómetros entre dos puntos geográficos en la tierra."""
    R = 6371.0 # Radio de la tierra en km
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c

class EventManager:
    """Gestor central para deduplicación espacial y temporal de sismos."""
    def __init__(self) -> None:
        self.state_file: str = "state_multi_poller.json"
        self.recent_events: List[Tuple[float, float, datetime, str]] = self.load_state()
        self.lock: asyncio.Lock = asyncio.Lock()
        
    def load_state(self) -> List[Tuple[float, float, datetime, str]]:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                
                events: List[Tuple[float, float, datetime, str]] = []
                for e in data:
                    time_utc = datetime.fromisoformat(e[2])
                    events.append((float(e[0]), float(e[1]), time_utc, str(e[3])))
                logger.info(f"Estado cargado con {len(events)} eventos recientes.")
                return events
            except Exception as e:
                logger.error(f"Error cargando estado: {e}")
        return []

    def save_state(self) -> None:
        try:
            data = [(e[0], e[1], e[2].isoformat(), e[3]) for e in self.recent_events]
            with open(self.state_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Error guardando estado: {e}")
            
    async def process_event(self, source: str, event_id: str, mag: float, lat: float, lon: float, time_utc: datetime, place: str) -> None:
        if mag < MIN_MAGNITUDE:
            return
            
        # Filtro Geográfico: Solo Sudamérica
        if not is_in_south_america(lat, lon):
            return

        # Ignorar sismos con más de 60 minutos de antigüedad
        now_utc = datetime.now(timezone.utc)
        if (now_utc - time_utc).total_seconds() > 3600:
            return
            
        async with self.lock:
            now = datetime.now(timezone.utc)
            self.recent_events = [e for e in self.recent_events if now - e[2] < timedelta(minutes=30)]
            
            is_duplicate = False
            for (r_lat, r_lon, r_time, r_id) in self.recent_events:
                if r_id == event_id:
                    is_duplicate = True
                    break
                dist = haversine_distance(lat, lon, r_lat, r_lon)
                time_diff = abs((time_utc - r_time).total_seconds())
                
                if dist < 100.0 and time_diff < 300: # 100 km, 5 minutos
                    is_duplicate = True
                    break
                    
            if not is_duplicate:
                self.recent_events.append((lat, lon, time_utc, event_id))
                self.save_state()
                await self.send_webhook_alert(source, mag, place, time_utc)

    async def send_webhook_alert(self, source: str, mag: float, place: str, time_utc: datetime) -> None:
        """Envía la alerta al dashboard web."""
        time_str = time_utc.isoformat()
        
        payload = {
            "timestamp": time_str,
            "station": f"{source} Global Feed",
            "mensaje": f"Magnitud: {mag:.1f} - Lugar: {place}",
            "mag": mag,
            "place": place,
            "is_real_data": True
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(WEBHOOK_URL, json=payload, timeout=5) as response:
                    response.raise_for_status()
                    logger.info(f"Alerta enviada a Web ({source}): M{mag} en {place}")
            # Prevención de colisiones rápidas al frontend
            await asyncio.sleep(1.0)
        except Exception as e:
            logger.error(f"Error enviando webhook desde {source}: {e}")

async def usgs_worker(manager: EventManager) -> None:
    logger.info("Iniciando USGS Worker...")
    last_ids: Set[str] = set()
    first_run = True
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(USGS_URL, timeout=10) as response:
                    data = await response.json()
                    features = data.get("features", [])
                    
                    if first_run and features:
                        # Enviar el más reciente como demo al inicio
                        latest = features[0]
                        eq_id = latest.get("id")
                        last_ids.add(eq_id)
                        
                        props = latest.get("properties", {})
                        mag = props.get("mag")
                        place = props.get("place", "Unknown")
                        time_ms = props.get("time")
                        coords = latest.get("geometry", {}).get("coordinates", [])
                        
                        if mag is not None and len(coords) >= 2 and time_ms:
                            time_utc = datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc)
                            logger.info("--- PRIMERA EJECUCIÓN USGS: Enviando el sismo más reciente para demostración ---")
                            await manager.process_event("USGS", eq_id, float(mag), float(coords[1]), float(coords[0]), time_utc, str(place))
                        
                        for f in features:
                            last_ids.add(f.get("id"))
                        first_run = False
                    else:
                        for feature in features:
                            eq_id = feature.get("id")
                            if eq_id and eq_id not in last_ids:
                                last_ids.add(eq_id)
                                props = feature.get("properties", {})
                                mag = props.get("mag")
                                place = props.get("place", "Unknown")
                                time_ms = props.get("time")
                                coords = feature.get("geometry", {}).get("coordinates", [])
                                
                                if mag is not None and len(coords) >= 2 and time_ms:
                                    time_utc = datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc)
                                    await manager.process_event("USGS", eq_id, float(mag), float(coords[1]), float(coords[0]), time_utc, str(place))
                                    
                    if len(last_ids) > 100:
                        last_ids = set(list(last_ids)[-50:])
        except Exception as e:
            logger.error(f"Error en USGS Worker: {e}")
            
        await asyncio.sleep(POLL_INTERVAL)

async def emsc_worker(manager: EventManager) -> None:
    logger.info("Iniciando EMSC Websocket Worker...")
    while True:
        try:
            async with websockets.connect(EMSC_WS_URL) as websocket:
                logger.info("EMSC Websocket Conectado.")
                async for message in websocket:
                    data = json.loads(message)
                    if data.get("action") == "create":
                        info = data.get("data", {}).get("properties", {})
                        mag = info.get("mag")
                        place = info.get("flynn_region", "Unknown")
                        time_str = info.get("time")
                        lat = info.get("lat")
                        lon = info.get("lon")
                        eq_id = info.get("source_id")
                        
                        if mag is not None and time_str and lat is not None and lon is not None and eq_id:
                            time_utc = datetime.strptime(time_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                            await manager.process_event("EMSC", str(eq_id), float(mag), float(lat), float(lon), time_utc, str(place))
        except Exception as e:
            logger.error(f"Error en EMSC Worker: {e}. Reconectando en 5s...")
            await asyncio.sleep(5)

async def gfz_worker(manager: EventManager) -> None:
    logger.info("Iniciando GFZ RSS Worker...")
    last_ids: Set[str] = set()
    first_run = True
    
    while True:
        try:
            feed = feedparser.parse(GFZ_RSS_URL)
            entries = feed.entries
            
            if first_run and entries:
                for entry in entries:
                    last_ids.add(entry.id)
                first_run = False
            else:
                for entry in entries:
                    eq_id = entry.id
                    if eq_id and eq_id not in last_ids:
                        last_ids.add(eq_id)
                        title = entry.title
                        parts = title.split(',', 1)
                        mag_str = parts[0].replace('M', '').strip()
                        place = parts[1].strip() if len(parts) > 1 else "Unknown"
                        
                        if hasattr(entry, 'geo_lat') and hasattr(entry, 'geo_long'):
                            lat = float(entry.geo_lat)
                            lon = float(entry.geo_long)
                            time_tuple = entry.published_parsed
                            
                            if time_tuple:
                                time_utc = datetime(*time_tuple[:6], tzinfo=timezone.utc)
                                try:
                                    mag = float(mag_str)
                                    await manager.process_event("GFZ", str(eq_id), mag, lat, lon, time_utc, str(place))
                                except ValueError:
                                    pass
                                    
            if len(last_ids) > 100:
                last_ids = set(list(last_ids)[-50:])
        except Exception as e:
            logger.error(f"Error en GFZ Worker: {e}")
            
        await asyncio.sleep(POLL_INTERVAL)

async def heartbeat_worker() -> None:
    """Envía un heartbeat cada 5 segundos al Dashboard web."""
    logger.info("Iniciando Heartbeat Worker...")
    while True:
        try:
            payload = {"type": "heartbeat", "time": datetime.now().strftime('%H:%M:%S')}
            async with aiohttp.ClientSession() as session:
                async with session.post(WEBHOOK_URL, json=payload, timeout=2) as response:
                    pass
        except Exception:
            pass # Ignorar errores de heartbeat si el frontend está caído
        await asyncio.sleep(5)

async def main() -> None:
    logger.info("=== INICIANDO MULTI POLLER PARA WEB ===")
    logger.info(f"Filtro Activo: Magnitud >= {MIN_MAGNITUDE}")
    
    manager = EventManager()
    
    await asyncio.gather(
        usgs_worker(manager),
        emsc_worker(manager),
        gfz_worker(manager),
        heartbeat_worker()
    )

if __name__ == "__main__":
    asyncio.run(main())
