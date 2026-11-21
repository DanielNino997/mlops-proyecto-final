"""Predicción rápida para una fila: mismos parámetros aprendidos, sin reconstruir el TargetEncoder en cada llamada.

sklearn's TargetEncoder.transform arma un diccionario con TODAS las categorías (decenas de miles de ciudades/zips)
en cada llamada: ~200 ms por petición. Aquí se arma una sola vez al cargar el modelo. El resultado es idéntico
(ver tests/test_api.py::test_fast_predictor_igual_al_modelo).
"""
import numpy as np
from sklearn.preprocessing import TargetEncoder


class FastPredictor:
    def __init__(self, model):
        self.model = model                       # TransformedTargetRegressor ya entrenado (log1p / expm1)
        pipe = model.regressor_
        self.estimator = pipe.named_steps["model"]
        self.parts = []
        for name, transformer, cols in pipe.named_steps["pre"].transformers_:
            if transformer == "drop":
                continue
            if transformer == "passthrough":
                self.parts.append(("num", list(cols), None))
            elif isinstance(transformer, TargetEncoder):
                maps = [dict(zip(cats.tolist(), enc.tolist()))
                        for cats, enc in zip(transformer.categories_, transformer.encodings_)]
                self.parts.append(("te", list(cols), (maps, float(transformer.target_mean_))))
            else:
                self.parts.append(("other", list(cols), transformer))

    def predict(self, X):
        blocks = []
        for kind, cols, extra in self.parts:
            if kind == "num":
                blocks.append(X[cols].to_numpy(dtype="float64"))
            elif kind == "te":
                maps, global_mean = extra       # categoría no vista -> media global (igual que TargetEncoder)
                blocks.append(np.array([[m.get(val, global_mean) for m, val in zip(maps, row)] for row in zip(*(X[c] for c in cols))],
                                       dtype="float64"))
            else:
                blocks.append(np.asarray(extra.transform(X[cols]), dtype="float64"))
        pred = self.estimator.predict(np.hstack(blocks))
        return self.model.transformer_.inverse_transform(pred.reshape(-1, 1)).ravel()
