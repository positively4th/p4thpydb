
from contrib.p4thpy.model import Model

from contrib.p4thpy.tools import Tools

class ColumnSpecModel(Model):

    def __init__(self, row, *args, **kwargs):
        super().__init__(row, *args, **kwargs)

    def transform(self, val, inverse=False):
        return val if 'transform' not in self else (self['transform'](val, inverse))

class TableSpecModel(Model):

    def __init__(self, row, *args, **kwargs):
        super().__init__(row, *args, **kwargs)

    def allColumns(self, sort=True, quote=True):
        return [column for column, spec in Tools.keyValIter(self['columnSpecs'], sort=sort)]
        #return [DB.quote(column, quote=quote) for column, spec in Tools.keyValIter(self['columnSpecs'], sort=sort)]

    def Ts(self, inverse=False):
        return {
            name: spec['transform'] for name, spec in Tools.keyValIter(self['columnSpecs'], sort=True) if 'transform' in spec
        }

    

class ORM:

    __DEBUG__ = False
    
    def __init__(self, db, pipe):
        self.db = db;
        self.pipe = pipe;

    def query(self, qpT, *args, **kwargs):
        return self.db.query(qpT, *args, **kwargs);
    
    def createTable(self, *args, **kwargs):
        self._createTable(*args, **kwargs)
        
    def ensureTable(self, tableSpec): 
        res = not self.tableExists(tableSpec)
        if res:
            self._createTable(tableSpec)

        self._createViews(tableSpec)
        return res
        
    def _createTable(self, tableSpec): 
        allColumns = [
            '{} {}'.format(self.db.quote(name), spec['definition']) for name, spec in tableSpec['columnSpecs'].items()
        ]
        primaryKeys = [
            '{}'.format(self.db.quote(key)) for key in tableSpec['primaryKeys']
        ]
        q = 'CREATE TABLE {} ({}, PRIMARY KEY ({}))'.format(self.db.quote(tableSpec['name']),
                                                                            ', '.join(allColumns),
                                                                            ', '.join(primaryKeys)
        )
        self.db.query((q, []))
        self._createViews(tableSpec)
        
    def _createViews(self, tableSpec):
        views = tableSpec['views'] if 'views' in tableSpec else {}
        for viewName, viewSpec in views.items():
            self._createView(viewName, viewSpec)
            
    def _createView(self, viewName, viewSpec):
        q = 'DROP VIEW IF EXISTS {}'.format(self.db.quote(viewName))
        self.query((q, []))
        q = 'CREATE VIEW {} AS {}'.format(self.db.quote(viewName), viewSpec['query'])
        self.query((q, []))
        
        
    def dropTable(self, tableSpec):
        q = 'DROP TABLE IF  EXISTS {}'.format(self.db.quote(tableSpec['name']))
        self.query((q, []))
        
    def tableExists(self, tableSpec):
        model = TableSpecModel(tableSpec)
        allColumns = ', '.join(self.db.quote(model.allColumns()))
        
        q = 'SELECT {} FROM {} LIMIT 1'.format(allColumns, self.db.quote(tableSpec['name']))
        p = {}
        try:
            self.query((q, p))
        except Exception as e:
            if self.__class__.__DEBUG__:
                print(e)
            return False
        return True

        
            
    def insert(self, tableSpec, rows, debug=False):
        model = TableSpecModel(tableSpec)
        models = {
            name: ColumnSpecModel(spec) for name, spec in Tools.keyValIter(tableSpec['columnSpecs'], sort=True)
        }
        allColumns = model.allColumns()
        #print(allColumns, 'allColumns')


        for row in rows:
            #print('row', row)
            columns = [col for col in allColumns if col in row]
            q = 'INSERT INTO {} ({}) VALUES ({})'.format(self.db.quote(tableSpec['name']), ', '.join(self.db.quote(columns)), ','.join(['?'] * len(columns)))
            self.query((q, [models[name].transform(row[name]) for name in columns]), debug=debug)

            
    def update(self, tableSpec, rows, debug=False):
        tsModel = TableSpecModel(tableSpec)
        valueColumns = set(tableSpec['columnSpecs'].keys()).difference(set(tableSpec['primaryKeys']))
        if len(valueColumns) < 1:
            return
        valSpecs = {
            col: ColumnSpecModel(tableSpec['columnSpecs'][col]) for i, col in Tools.keyValIter(valueColumns, sort=True)
        }
        keySpecs = {
            col: ColumnSpecModel(tableSpec['columnSpecs'][col]) for i, col in Tools.keyValIter(tableSpec['primaryKeys'], sort=True)
        }
        for row in rows:
            valAssigns = ['{} = ?'.format(self.db.quote(col)) for col, spec in Tools.keyValIter(valSpecs, sort=True) if col in row]
            keyWheres = ['{} = ?'.format(self.db.quote(col)) for col, spec in Tools.keyValIter(keySpecs, sort=True) if col in row]
        
            q = 'UPDATE {} SET {} WHERE {}'.format(self.db.quote(tableSpec['name']),
                                               ','.join(valAssigns),
                                               ' AND '.join(keyWheres))
            p = [
                spec.transform(row[key], inverse=False) for key, spec in Tools.keyValIter(valSpecs, sort=True) if key in row
            ] + [
                spec.transform(row[key], inverse=False) for key, spec in Tools.keyValIter(keySpecs, sort=True) if key in row
            ]
            self.query((q, p), debug=debug)
        
    def upsert(self, tableSpec, rows, batchSize=200, debug=False):
        tsModel = TableSpecModel(tableSpec)

        completed = 0
        while completed < len(rows):
            batch = rows[completed:completed+batchSize]

            keyMaps = [Tools.sortByKeys({
                col: ColumnSpecModel(tableSpec['columnSpecs'][col]).transform(row[col]) for i, col in Tools.keyValIter(tableSpec['primaryKeys'])
                }) for row in batch]

            qpT = self.select(tableSpec)
            #print(qpT, 'qpT')
            #assert 1 == 0
            qpT = self.pipe.concat(qpT, pipes=[
                [self.pipe.any, {
                    'pipes': [
                        [self.pipe.equals, {
                            'map': keyMap,
                            'quote': True,
                        }] for keyMap in keyMaps
                    ]
                }],
                [self.pipe.columns, {'columns': tableSpec['primaryKeys'], 'quote': True}],
            ])
            existingKeys = self.query(qpT, debug=False)
            #print('existingKeys', existingKeys)
            insertRows = []
            updateRows = []

            for i,row in enumerate(batch): 
                keyMap = keyMaps[i]
                try:
                    keyIndex = existingKeys.index(keyMap)
                    updateRows.append(row)
                    existingKeys.pop(keyIndex)
                except ValueError as e:
                    #print('----')
                    #print(existingKeys)
                    #print(keyMap)
                    #print('----')
                    insertRows.append(row)
            #print('Inserts:', len(insertRows), 'Updates:', len(updateRows))
            self.insert(tableSpec, insertRows, debug=debug)
            self.update(tableSpec, updateRows, debug=debug)

            completed = completed + len(batch)

    def join(self, qpT, relatedSpec): 

        
        q,p,T = self.db.__qpTSplit__(qpT)

        #print(relatedSpec)
        onMap = [
            '_r.{} = _l.{}'.format(key, fKey) for fKey, key in relatedSpec['foreignKeyMap'].items()
        ]
        aliases = [
            '{} {}'.format(expr, self.db.quote(alias)) for alias, expr in relatedSpec['columns'].items()
        ]
        q = 'SELECT _l.*, {} FROM ({}) _l'.format(', '.join(aliases), q)
        q = '{} INNER JOIN ({}) _r ON {}'.format(q, relatedSpec['select'], ' AND '.join(onMap))
        return q,p, T
                                               
        

    def select(self, tableSpec):
        tss = TableSpecModel(tableSpec)
        models = {
            name: ColumnSpecModel(spec) for name, spec in tableSpec['columnSpecs'].items()
        }
        allColumns = ', '.join(self.db.quote(tss.allColumns()))
        q = 'SELECT {} FROM {}'.format(allColumns, self.db.quote(tableSpec['name']))
        p = []
        T = tss.Ts()
        if 'related' in tableSpec:
            print('Related is deprecated, use views instead!')
            relatedSpecs = tableSpec['related'] if 'related' in tableSpec else []
            for relatedSpec in relatedSpecs:
                q,p,T = self.join((q, p, T), relatedSpec)
        return q, p, T

    def view(self, tableSpec, view):
        viewSpec = tableSpec['views'][view]
        
        tss = TableSpecModel(tableSpec)
        csModels = {
            name: ColumnSpecModel(spec) for name, spec in viewSpec['columnSpecs'].items()
        }
        allColumns = ', '.join(self.db.quote(tss.allColumns()))

        
        q = 'SELECT {} FROM {}'.format(allColumns, self.db.quote(view))
        p = []
        T = {name: cs['transform'] for name, cs in Tools.keyValIter(viewSpec['columnSpecs'], sort=True)}
        return q, p, T

        
    @staticmethod
    def allColumns(tableSpec):
        return [
            '"{}"'.format(columnSpec[column]) for column, spec in Tools.keyValIter(tableSpec, sort=True)
        ]

    
