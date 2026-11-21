import sys
from pathlib import Path

# Igual que en Airflow: la carpeta dags/ está en el path, así se importa "lib.*"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
