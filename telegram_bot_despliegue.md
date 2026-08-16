# Guía de Despliegue: Bot de Alertas Sísmicas (USGS) en NAS con Docker

Esta guía explica cómo desplegar un contenedor Docker en tu NAS que consultará ininterrumpidamente el feed mundial de sismos del **USGS** y te enviará una notificación a Telegram cada vez que ocurra un sismo igual o mayor a **M4.5**.

## 1. Arquitectura del Sistema

- **Servicio Analizador:** Un script en Python (`bot.py`) que se ejecuta 24/7.
- **Fuente de Datos:** Consume el GeoJSON público del USGS (`earthquake.usgs.gov`) cada 60 segundos.
- **Notificación:** Usa la API oficial de Telegram para enviar un mensaje con formato Markdown a tu Chat Personal o Grupo.

---

## 2. Preparación del Entorno (NAS)

Accede a tu NAS por SSH y crea el directorio base:

```bash
mkdir -p telegram_quake_bot
cd telegram_quake_bot
```

Dentro de este directorio, deberás crear tres archivos:
1. `bot.py`
2. `requirements.txt`
3. `Dockerfile`
4. `docker-compose.yml`

*(Estos archivos ya han sido generados en tu proyecto local, solo tienes que copiarlos a tu NAS).*

---

## 3. Configuración del Orquestador

El archivo principal que controla todo es el `docker-compose.yml`.
Puedes ajustar los siguientes parámetros en la sección `environment`:

- `TELEGRAM_BOT_TOKEN`: El token de tu bot de Telegram.
- `TELEGRAM_CHAT_ID`: El ID de tu chat donde recibirás los mensajes.
- `MIN_MAGNITUDE`: Magnitud mínima para alertar (por defecto `4.5`). Si quieres que te avise de cosas más pequeñas, bájalo a `4.0` o `3.0`.
- `POLL_INTERVAL`: Tiempo en segundos entre cada chequeo al USGS (por defecto `60`).
- `TZ`: Zona horaria, ej. `America/Lima`.

---

## 4. Despliegue y Ejecución

Una vez que tengas los 4 archivos en el NAS, ejecuta:

```bash
docker compose up -d --build
```

### Verificación

Para asegurarte de que el bot arrancó correctamente y está pre-cargando la base de datos de sismos inicial, revisa los logs:

```bash
docker logs -f usgs_quake_bot
```

Deberías ver algo como:
```
Iniciando Bot Centinela USGS...
Filtro: Magnitud >= 4.5
Intervalo de escaneo: 60 segundos
Pre-cargando sismos existentes...
Pre-cargados 45 sismos. Listo para monitorear.
```

A partir de este momento, el bot vivirá en tu NAS y cada vez que el USGS reporte un nuevo sismo de Magnitud $\ge$ 4.5 en el mundo, tu celular sonará con una notificación de Telegram y un enlace a Google Maps.
