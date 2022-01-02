
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
    
    def __init__(self, db, util, pipe):
        self.db = db;
        self.pipe = pipe;
        self.util = util

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
            '{} {}'.format(self.util.quote(name), spec['definition']) for name, spec in tableSpec['columnSpecs'].items()
        ]
        primaryKeys = [
            '{}'.format(self.util.quote(key)) for key in tableSpec['primaryKeys']
        ]

        q = 'CREATE TABLE {} ({}, PRIMARY KEY ({}))'.format(self.util.quote(tableSpec['name']),
                                                                            ', '.join(allColumns),
                                                                            ', '.join(primaryKeys)
        )
        self.db.query((q, []), debug=False)
        self._createViews(tableSpec)
        
    def _createViews(self, tableSpec):
        views = tableSpec['views'] if 'views' in tableSpec else {}
        for viewName, viewSpec in views.items():
            self._createView(viewName, viewSpec)
            
    def _createView(self, viewName, viewSpec):
        q = 'DROP VIEW IF EXISTS {}'.format(self.util.quote(viewName))
        self.query((q, []))
        q = 'CREATE VIEW {} AS {}'.format(self.util.quote(viewName), viewSpec['query'])
        self.query((q, []))
        
        
    def dropTable(self, tableSpec):
        self.dropViews(tableSpec)
        q = 'DROP TABLE IF  EXISTS {}'.format(self.util.quote(tableSpec['name']))
        self.query((q, []), debug=False)
        
    def dropViews(self, tableSpec):
        for viewName, viewSpec in tableSpec['views'].items():
            self.dropView(viewName)
        
    def dropView(self, viewName):
        q = 'DROP VIEW IF  EXISTS {}'.format(self.util.quote(viewName))
        self.query((q, []))
        
    def tableExists(self, tableSpec):
        model = TableSpecModel(tableSpec)
        allColumns = ', '.join(self.util.quote(model.allColumns()))

        return self.db.tableExists(tableSpec['name'], model.allColumns());
            
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
            q = 'INSERT INTO {} ({}) VALUES ({})'.format(self.util.quote(tableSpec['name']), ', '.join(self.util.quote(columns)), ','.join([self.ph] * len(columns)))
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
            valAssigns = ['{} = {}'.format(self.util.quote(col), self.ph) for col, spec in Tools.keyValIter(valSpecs, sort=True) if col in row]
            keyWheres = ['{} = {}'.format(self.util.quote(col), self.ph) for col, spec in Tools.keyValIter(keySpecs, sort=True) if col in row]
        
            q = 'UPDATE {} SET {} WHERE {}'.format(self.util.quote(tableSpec['name']),
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

        
        q,p,T = self.util.qpTSplit(qpT)

        #print(relatedSpec)
        onMap = [
            '_r.{} = _l.{}'.format(key, fKey) for fKey, key in relatedSpec['foreignKeyMap'].items()
        ]
        aliases = [
            '{} {}'.format(expr, self.util.quote(alias)) for alias, expr in relatedSpec['columns'].items()
        ]
        q = 'SELECT _l.*, {} FROM ({}) _l'.format(', '.join(aliases), q)
        q = '{} INNER JOIN ({}) _r ON {}'.format(q, relatedSpec['select'], ' AND '.join(onMap))
        return q,p, T
                                               

    #Todo: Write more efficient method in db specific orm classes.
    def delete(self, tableSpec, keyMaps):
        tss = TableSpecModel(tableSpec)

        p = {}
        keyMapsT = [
            {
                col: ColumnSpecModel(tableSpec['columnSpecs'][col]).transform(val)
                for col, val in Tools.keyValIter(keyMap)
            } for keyMap in keyMaps
        ]


        table = self.util.quote(tableSpec['name']) 
        keys = [self.util.quote(key) for key in tableSpec['primaryKeys']]

        qs = [
            self.pipe.equals(('SELECT {} FROM {}'.format(','.join(keys), table), []), map=keyMap)
            for keyMap in keyMapsT
        ]

        p = {}
        q = 'SELECT * FROM {}'.format(table)
        q, p, T = self.pipe.any((q,p, None), pipes=[
            [self.pipe.equals, { 'map': keyMapT, } ]
            for keyMapT in keyMapsT
        ])

        res = self.query((q, p, T))

        existAlias = '_eq'
        q = 'SELECT {} FROM ({}) AS {}'.format(', '.join(keys), q, existAlias)
        where = ' AND '.join([
                '{table}.{key} = {alias}.{key}'.format(table=table, alias=existAlias, key=key) for key in keys
            ]) 

        q = 'DELETE FROM {} WHERE EXISTS ({} WHERE {}) RETURNING *'.format(table, q, where)
        T = tss.Ts()

        self.query((q, p, T,))
        return res

    def select(self, tableSpec):
        tss = TableSpecModel(tableSpec)
        models = {
            name: ColumnSpecModel(spec) for name, spec in tableSpec['columnSpecs'].items()
        }
        allColumns = ', '.join(self.util.quote(tss.allColumns()))
        q = 'SELECT {} FROM {}'.format(allColumns, self.util.quote(tableSpec['name']))
        p = []
        T = tss.Ts()
        return q, p, T

    def view(self, tableSpec, view):
        viewSpec = tableSpec['views'][view]
        
        tss = TableSpecModel(tableSpec)
        csModels = {
            name: ColumnSpecModel(spec) for name, spec in viewSpec['columnSpecs'].items()
        }
        allColumns = ', '.join(self.util.quote(tss.allColumns()))

        
        q = 'SELECT {} FROM {}'.format(allColumns, self.util.quote(view))
        p = []
        T = {name: cs['transform'] for name, cs in Tools.keyValIter(viewSpec['columnSpecs'], sort=True)}
        return q, p, T

        
    @staticmethod
    def allColumns(tableSpec):
        return [
            '"{}"'.format(columnSpec[column]) for column, spec in Tools.keyValIter(tableSpec, sort=True)
        ]

    @property
    def ph(self):
        return self.util.placeholder
    
