"""Cliente de la API de datos del proyecto final (lotes de propiedades por grupo)."""
import logging
import time

import requests

log = logging.getLogger(__name__)

RETRY_STATUS = {500, 502, 503, 504}
END_OF_DATA_TEXT = "toda la informaci"  # detalle del HTTP 400 cuando ya no hay más lotes


class DataApiError(Exception):
    """Fallo real al obtener un lote: debe hacer fallar la tarea."""


class NoMoreBatches(Exception):
    """La API indicó que ya se entregaron todos los lotes: fin normal, no es error."""


def fetch_batch(base_url, group_number, timeout=(5, 120), max_retries=3,
                backoff=2.0, session=None, sleep=time.sleep):
    """Pide el siguiente lote del grupo. Devuelve el JSON con group_number, batch_number y data."""
    http = session or requests.Session()
    url = f"{base_url.rstrip('/')}/data"
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = http.get(url, params={"group_number": group_number}, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code not in RETRY_STATUS:
                return _parse_response(resp, group_number)
            last_error = f"HTTP {resp.status_code}"

        if attempt < max_retries:
            wait = backoff * 2 ** (attempt - 1)
            log.warning("Intento %s/%s falló (%s). Reintentando en %.0f s",
                        attempt, max_retries, last_error, wait)
            sleep(wait)

    raise DataApiError(f"La API de datos falló tras {max_retries} intentos: {last_error}")


def _parse_response(resp, group_number):
    if resp.status_code == 400 and END_OF_DATA_TEXT in _detail(resp).lower():
        raise NoMoreBatches(_detail(resp))
    if resp.status_code != 200:
        raise DataApiError(f"HTTP {resp.status_code}: {resp.text[:200]}")

    try:
        body = resp.json()
    except ValueError as exc:
        raise DataApiError("La respuesta no es JSON válido") from exc

    missing = {"group_number", "batch_number", "data"} - set(body)
    if missing:
        raise DataApiError(f"Faltan campos en la respuesta: {sorted(missing)}")
    if body["group_number"] != group_number:
        raise DataApiError(f"Se pidió el grupo {group_number} y llegó el {body['group_number']}")
    if not isinstance(body["data"], list) or not body["data"]:
        raise DataApiError(f"El lote {body['batch_number']} llegó vacío")

    log.info("Lote %s del grupo %s recibido: %s filas",
             body["batch_number"], group_number, len(body["data"]))
    return body


def _detail(resp):
    try:
        return str(resp.json().get("detail", ""))
    except ValueError:
        return resp.text
