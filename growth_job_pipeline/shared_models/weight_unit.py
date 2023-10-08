from enum import Enum


# if different units are used in future, might need normalisation to single output unit e.g. g -> kg, lbs -> kg etc
class WeightUnit(str, Enum):
    """
    Represents allowed weight unit for yields
    """

    kg = "kg"
