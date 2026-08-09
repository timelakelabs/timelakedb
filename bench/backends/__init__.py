from .base import Backend, WriteError
from .timelakedb import TimeLakeDB
from .influxdb3 import InfluxDB3
from .influxdb2 import InfluxDB2
from .questdb import QuestDB
from .victoriametrics import VictoriaMetrics
from .influxdb1 import InfluxDB1

BACKENDS = {cls.name: cls for cls in (TimeLakeDB, InfluxDB3, InfluxDB2,
                                      QuestDB, VictoriaMetrics, InfluxDB1)}


def get_backend(name):
    if name not in BACKENDS:
        raise KeyError(f"unknown backend '{name}' "
                       f"(available: {', '.join(sorted(BACKENDS))})")
    return BACKENDS[name]
