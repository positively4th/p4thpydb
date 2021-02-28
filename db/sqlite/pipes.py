from ..pipes import Pipes as Pipes0
from .util import Util

class Pipes(Pipes0):

    def __init__(self, *args, **kwargs):
        util = Util()
        super().__init__(util)


    def matches(self, qpT, map=None, quote=True, *args, **kwargs):
        return self.equals(qpT, map=map, quote=quote, op='REGEXP', *args, **kwargs)

