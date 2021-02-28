from uuid import uuid4

class DBError(Exception):
    pass

class DB:

    __DEBUG__ = False

    def __init__(self, util):
        self.util = util
        self.savepoints = []

    @classmethod
    def dbgout(cls, msg, debug=None):
        if debug or (debug is None and cls.__DEBUG__):
            if isinstance(msg, str):
                print(msg)
            else:
                print(*msg)
        return msg

    def exportToFile(self, path, invert=False):

        raise DBError('Not implemented')
   
    def startTransaction(self):
        #print('startTransaction', self.savepoints)
        id = str(uuid4())
        self.query('SAVEPOINT "{}"'.format(id), debug=None);
        self.savepoints.append(id)
        
    def rollback(self):
        #print('rollback', self.savepoints)
        if len(self.savepoints) < 1:
            return
        id = self.savepoints.pop()
        self.query(('ROLLBACK TO "{}"'.format(id)),debug=None);
        self.query(('RELEASE "{}"'.format(id)), debug=None);
        
    def commit(self):
        #print('commit', self.savepoints)
        id = self.savepoints.pop()
        self.query(('RELEASE "{}"'.format(id)), debug=None);



    
