from enum import Enum


class Crop(str, Enum):
    """
    Represents allowed crops
    Extension needed in future as different crops are added
    """

    basil = "basil"
    chille = "chille"
    potato = "potato"
    brocolli = "brocolli"
