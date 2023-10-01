from enum import Enum


# extension likely needed in future for different crops
class Crop(str, Enum):
    basil = "basil"
    chille = "chille"
    potato = "potato"
    brocolli = "brocolli"
