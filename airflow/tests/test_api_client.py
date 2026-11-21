import pytest
import requests

from lib.api_client import DataApiError, NoMoreBatches, fetch_batch

URL = "http://api-datos"
LOTE_OK = {"group_number": 2, "batch_number": 0,
           "data": [{"price": 289900.0, "state": "Connecticut"}]}
FIN = {"detail": "Ya se recolectó toda la información minima necesaria"}


def sin_espera(_):
    pass


def test_lote_correcto(requests_mock):
    requests_mock.get(f"{URL}/data", json=LOTE_OK)
    body = fetch_batch(URL, 2, sleep=sin_espera)
    assert body["batch_number"] == 0 and len(body["data"]) == 1


def test_reintenta_un_500_y_luego_funciona(requests_mock):
    requests_mock.get(f"{URL}/data", [{"status_code": 500}, {"json": LOTE_OK}])
    fetch_batch(URL, 2, sleep=sin_espera)
    assert requests_mock.call_count == 2


def test_timeout_persistente_falla_tras_3_intentos(requests_mock):
    requests_mock.get(f"{URL}/data", exc=requests.exceptions.ConnectTimeout)
    with pytest.raises(DataApiError, match="3 intentos"):
        fetch_batch(URL, 2, sleep=sin_espera)
    assert requests_mock.call_count == 3


def test_fin_de_datos_no_reintenta(requests_mock):
    requests_mock.get(f"{URL}/data", status_code=400, json=FIN)
    with pytest.raises(NoMoreBatches):
        fetch_batch(URL, 2, sleep=sin_espera)
    assert requests_mock.call_count == 1


def test_otro_400_es_error_real(requests_mock):
    requests_mock.get(f"{URL}/data", status_code=422, json={"detail": "group_number inválido"})
    with pytest.raises(DataApiError, match="422"):
        fetch_batch(URL, 2, sleep=sin_espera)


def test_lote_vacio(requests_mock):
    requests_mock.get(f"{URL}/data", json={**LOTE_OK, "data": []})
    with pytest.raises(DataApiError, match="vacío"):
        fetch_batch(URL, 2, sleep=sin_espera)


def test_respuesta_no_json(requests_mock):
    requests_mock.get(f"{URL}/data", text="<html>error</html>")
    with pytest.raises(DataApiError, match="JSON"):
        fetch_batch(URL, 2, sleep=sin_espera)
