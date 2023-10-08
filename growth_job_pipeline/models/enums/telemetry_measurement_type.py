from enum import Enum


class TelemetryMeasurementType(str, Enum):
    """
    Represents allowed telemetry measurement types
    """

    temp = "temp"
