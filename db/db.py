from uuid import uuid4

import logging
log0 = logging.getLogger(__name__)

class DBError(Exception):
    pass

class DB:

    def __init__(self, util, log=log0):
        self.util = util
        self._log = log
        self.savepoints = []

    @property
    def log(self):
        return log0 if self._log is None else self._log
    
    def exportToFile(self, path, invert=False, explain=False, schemas=None, restoreTables=None):
        raise DBError('Not implemented')
   
    def startTransaction(self):
        self.log.debug('startTransaction (%s)' % len(self.savepoints))
        id = str(uuid4())
        self.query('SAVEPOINT "{}"'.format(id), debug=None);
        self.savepoints.append(id)
        
    def rollback(self):
        self.log.debug('rollback (%s)' % len(self.savepoints))
        if len(self.savepoints) < 1:
            return
        id = self.savepoints.pop()
        self.query(('ROLLBACK TO "{}"'.format(id)),debug=None);
        self.query(('RELEASE "{}"'.format(id)), debug=None);
        
    def commit(self):
        self.log.debug('commit (%s)' % len(self.savepoints))
        id = self.savepoints.pop()
        self.query(('RELEASE "{}"'.format(id)), debug=None);



    
