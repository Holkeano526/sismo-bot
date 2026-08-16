import asyncio
import aiohttp
import websockets
import feedparser
import json
import math
import os
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Tuple, Set

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SismoBot")

# Configuración desde variables de entorno
TELEGRAM_BOT_TOKEN: str | None = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str | None = os.environ.get("TELEGRAM_CHAT_ID")
MIN_MAGNITUDE: float = float(os.environ.get("MIN_MAGNITUDE", "4.0"))
POLL_INTERVAL: int = int(os.environ.get("POLL_INTERVAL", "60"))
TIMEZONE_STR: str = os.environ.get("TZ", "America/Lima")

# Fuentes de Datos
USGS_URL: str = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
EMSC_WS_URL: str = "wss://www.seismicportal.eu/standing_order/websocket"
GFZ_RSS_URL: str = "https://geofon.gfz-potsdam.de/eqinfo/list.php?fmt=rss"

def is_in_south_america(lat: float, lon: float) -> bool:
    """Verifica si las coordenadas están aproximadamente dentro de Sudamérica."""
    return -60.0 <= lat <= 15.0 and -85.0 <= lon <= -30.0

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula la distancia en kilómetros entre dos puntos geográficos en la tierra.
    
    Args:
        lat1 (float): Latitud del punto 1.
        lon1 (float): Longitud del punto 1.
        lat2 (float): Latitud del punto 2.
        lon2 (float): Longitud del punto 2.
        
    Returns:
        float: Distancia en kilómetros.
    """
    R = 6371.0 # Radio de la tierra en km
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance

class EventManager:
    """
    Gestor central de eventos sísmicos que se encarga de la deduplicación espacial 
    y temporal de sismos reportados por diferentes agencias.
    """
    def __init__(self) -> None:
        self.state_file: str = os.environ.get("STATE_FILE", "state.json")
        # recent_events almacena: (latitud, longitud, tiempo_utc, event_id)
        self.recent_events: List[Tuple[float, float, datetime, str]] = self.load_state()
        self.lock: asyncio.Lock = asyncio.Lock()
        
    def load_state(self) -> List[Tuple[float, float, datetime, str]]:
        """Carga el estado de los últimos sismos desde el almacenamiento en disco."""
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
        """Serializa y guarda el estado de los últimos sismos en el almacenamiento en disco."""
        try:
            data = [(e[0], e[1], e[2].isoformat(), e[3]) for e in self.recent_events]
            with open(self.state_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Error guardando estado: {e}")
            
    async def process_event(self, source: str, event_id: str, mag: float, lat: float, lon: float, time_utc: datetime, place: str) -> None:
        """
        Procesa un evento, aplica filtro de magnitud, filtro geográfico, deduplicación espacial/temporal y envía a Telegram.
        """
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
            # Limpiar eventos más antiguos que 30 minutos
            now = datetime.now(timezone.utc)
            self.recent_events = [e for e in self.recent_events if now - e[2] < timedelta(minutes=30)]
            
            # Verificar si es duplicado (mismo evento, otro proveedor)
            # Criterio: A menos de 100km de distancia Y en una ventana de +/- 5 minutos del tiempo original
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
                await self.send_telegram_alert(source, mag, place, time_utc, lat, lon)

    async def send_telegram_alert(self, source: str, mag: float, place: str, time_utc: datetime, lat: float, lon: float) -> None:
        """
        Envía un mensaje formateado con Markdown a la API de Telegram.
        Incluye un control de Rate Limiting para evitar baneos por Spam (HTTP 429).
        """
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.warning(f"[{source}] Telegram no configurado. M{mag} en {place}")
            return

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        
        # Convertir UTC a la zona horaria local (Peru)
        local_time = time_utc.astimezone(ZoneInfo(TIMEZONE_STR))
        time_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
        
        maps_link = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
        
        emoji = "🔴" if mag >= 6.0 else "🟠" if mag >= 5.0 else "🟡"
        
        mensaje = (
            f"{emoji} *¡ALERTA SÍSMICA DETECTADA!* {emoji}\n\n"
            f"📊 *Magnitud:* {mag:.1f}\n"
            f"📍 *Ubicación:* {place}\n"
            f"⏱ *Hora:* {time_str}\n\n"
            f"[🗺️ Ver en Google Maps]({maps_link})\n\n"
            f"📡 *Fuente:* {source}"
        )
        
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": mensaje,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as response:
                    response.raise_for_status()
                    logger.info(f"Alerta enviada ({source}): M{mag} en {place}")
            
            # Prevención de Spam: Esperar 1.5s para respetar límite de Telegram de 1 msg/segundo
            await asyncio.sleep(1.5)
        except Exception as e:
            logger.error(f"Error enviando a Telegram desde {source}: {e}")

async def usgs_worker(manager: EventManager) -> None:
    """Worker asíncrono que sondea el feed GeoJSON de USGS periódicamente."""
    logger.info("Iniciando USGS Worker...")
    last_ids: Set[str] = set()
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(USGS_URL, timeout=10) as response:
                    data = await response.json()
                    for feature in data.get("features", []):
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
    """Worker asíncrono que mantiene una conexión Websocket con EMSC para notificaciones instantáneas."""
    logger.info("Iniciando EMSC Websocket Worker...")
    
    while True:
        try:
            async with websockets.connect(EMSC_WS_URL) as websocket:
                logger.info("EMSC Websocket Conectado exitosamente.")
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
                            # Formato típico: "2023-11-20T12:30:00.0Z"
                            time_utc = datetime.strptime(time_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                            await manager.process_event("EMSC", str(eq_id), float(mag), float(lat), float(lon), time_utc, str(place))
        except Exception as e:
            logger.error(f"Error en EMSC Worker: {e}. Reconectando en 5s...")
            await asyncio.sleep(5)

async def gfz_worker(manager: EventManager) -> None:
    """Worker asíncrono que sondea el feed RSS de GEOFON (GFZ) periódicamente."""
    logger.info("Iniciando GFZ RSS Worker...")
    last_ids: Set[str] = set()
    
    while True:
        try:
            feed = feedparser.parse(GFZ_RSS_URL)
            for entry in feed.entries:
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

async def main() -> None:
    """Punto de entrada principal para la aplicación SismoBot."""
    logger.info("=== INICIANDO BOT MULTI-PROVEEDOR ===")
    logger.info(f"Filtro Activo: Magnitud >= {MIN_MAGNITUDE}")
    
    manager = EventManager()
    
    # Iniciar los tres workers concurrentemente
    await asyncio.gather(
        usgs_worker(manager),
        emsc_worker(manager),
        gfz_worker(manager)
    )

if __name__ == "__main__":
    asyncio.run(main())
