
from .db import DB

from ..orm_async import ORM as ORM0
from .pipes import Pipes
from .util import Util


class ORM(ORM0):

    __DEBUG__ = False

    def __init__(self, db: DB):
        super().__init__(db, Util(), Pipes())
