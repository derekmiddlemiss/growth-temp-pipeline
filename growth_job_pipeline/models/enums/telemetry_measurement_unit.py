from enum import Enum


class TelemetryMeasurementUnit(str, Enum):
    """
    Represents allowed telemetry measurement units
    """

    C = "C"
    F = "F"
