import re
from ..util import Util as Util0

class Util(Util0):

    def __init__(self):
        super().__init__(':', '', '`', '?')

    @classmethod
    def parseIndexTableName(cls, fqn):
        return re.sub(r'^(.*?.)[.](.*?)$', r'\2', fqn)
