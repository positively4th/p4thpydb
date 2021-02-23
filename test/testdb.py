import unittest

class TestDBCompare(unittest.TestCase):

    def test_legacy(self):

        import tempfile
        import pytest
        from collections import OrderedDict

        from db.sqlite.db import DB
        from db.p import P
        from db.ts import Ts
        from db.orm import ORM
        from db.pipe import Pipe

        #P
        q = '''
    SELECT :p1, p2
    FROM a as _t
    WHRER :_p3 > :5p
    '''
        p = {
            'p1': 1,
            'p2': 2,
            '_p3': 3,
            'p4': 4,
            '5p': 5,
        }
        pStripped = P.pStrip(q, p)
        #print(pStripped)
        assert {
            'p1': 1,
            '_p3': 3,
            '5p': 5
        } == pStripped

        #DB
        #dbFile = os.path.join(tempfile.gettempdir(), str(uuid4()))
        dbFile = ':memory:'
        tmpSchemaFile = ':memory:'
        #tmpSchemaFile = '/tmp/testattached {}.db'.format(str(uuid4))
        db = DB(dbFile, attaches={
            't mp': tmpSchemaFile,
        })
        pipe = Pipe();
        orm = ORM(db, pipe);
        db.query('CREATE TABLE strvec3 (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))')
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)");

        db.query('CREATE TABLE {} (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))'.format(db.quote('t mp._tstrvec3')), debug=False)
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('111', 10, 10, 10)".format(db.quote('t mp._tstrvec3')));
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('123', 10, 20, 30)".format(db.quote('t mp._tstrvec3')));
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('222', 20, 20, 20)".format(db.quote('t mp._tstrvec3')));
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('321', 30, 20, 10)".format(db.quote('t mp._tstrvec3')));
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('333', 30, 30, 30)".format(db.quote('t mp._tstrvec3')));

        #attached
        row = db.query("SELECT * FROM {} WHERE id = '321'".format(db.quote('t mp._tstrvec3')));
        assert len(row) == 1
        row = row[0]
        assert {
            'id': '321',
            'a': 30,
            'b': 20,
            'c': 10,
        } == row


        #quote
        col = 'alfa'
        assert db.quote(col) == '`alfa`'
        assert db.quote(col, False) == 'alfa'

        cols = ['alfa', 'beta']
        #print('cols', db.quote(cols))
        assert db.quote(cols) == ['`alfa`', '`beta`']
        assert db.quote(cols, False) == ['alfa', 'beta']


        ##ORM

        #createTable, dropTable, insert, update
        tableSpec = {
            'name': "apifootballteam_v0",
            'columnSpecs': {
                'team_id': {
                    'definition': "TEXT NOT NULL",
                    'transform': Ts.str,
                },
                'name': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                },
                'country': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                },
                'verified': {
                    'definition': "INT NOT NULL DEFAULT 0",
                    'transform': Ts.boolAsInt,
                },
            },
            'primaryKeys': ["team_id"],
            'views': OrderedDict({}),
        }
        tableSpec['views']['prefixed'] = {
            'query': "SELECT {team_id}, '_' || {name} {name}, {country}, {verified} FROM {table}".format(
                team_id=db.quote('team_id'),
                name=db.quote('name'),
                country=db.quote('country'),
                verified=db.quote('verified'),
                table=db.quote(tableSpec['name']),
            ),
            'columnSpecs': {
                'team_id': {
                    'definition': "TEXT NOT NULL",
                    'transform': Ts.str,
                },
                'name': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': lambda n, *_: '_' + Ts.str(n,True), #prefixed by _ twice!
                },
                'country': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                },
                'verified': {
                    'definition': "INT NOT NULL DEFAULT 0",
                    'transform': Ts.boolAsInt,
                },
            },
        }

        #print(tableSpec['views'])
        #DB.__DEBUG__ = True
        orm.createTable(tableSpec)
        #print(tableSpec['columnSpecs'])
        assert True == orm.tableExists(tableSpec)
        assert False == orm.ensureTable(tableSpec)
        orm.dropTable(tableSpec)
        assert True == orm.ensureTable(tableSpec)
        assert True == orm.tableExists(tableSpec)
        assert False == orm.ensureTable(tableSpec)
        assert True == orm.tableExists(tableSpec)
        orm.insert(tableSpec, [
            { 'team_id': 't1', 'name': 'n1', 'country': '1', 'verified': True, },
            { 'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': 0, }
        ], debug=True)
        #assert 1 == 0
        rows = db.query(orm.select(tableSpec))
        rows.sort(key=lambda row: row['team_id'])
        assert 'n1' == rows[0]['name']
        assert True == rows[0]['verified']
        assert 'n2' == rows[1]['name']
        assert False == rows[1]['verified']

        rows = db.query(orm.view(tableSpec, view='prefixed'))
        rows.sort(key=lambda row: row['team_id'])
        assert '__n1' == rows[0]['name']
        assert True == rows[0]['verified']
        assert '__n2' == rows[1]['name']
        assert False == rows[1]['verified']

        orm.update(tableSpec, [
            { 'team_id': 't1', 'verified': False, },
            { 'team_id': 't2', 'verified': 1, }
        ])
        rows = db.query(orm.select(tableSpec))
        rows.sort(key=lambda row: row['team_id'])
        assert 'n1' == rows[0]['name']
        assert False == rows[0]['verified']
        assert 'n2' == rows[1]['name']
        assert True == rows[1]['verified']

        orm.dropTable(tableSpec)

        #upsert Todo: Test with composite keys!
        orm.createTable(tableSpec)
        assert True == orm.tableExists(tableSpec)

        orm.upsert(tableSpec, [
            { 'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': 0, 'index': 0}
        ])
        rows = db.query(orm.select(tableSpec))
        rows.sort(key=lambda row: row['team_id'])
        assert len(rows) == 1
        assert 'n2' == rows[0]['name']
        assert False == rows[0]['verified']

        orm.upsert(tableSpec, [
            { 'team_id': 't1', 'name': 'nn1', 'country': '1', 'verified': 0, 'index': 10},
            { 'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': True, 'index': 20}
        ])
        rows = db.query(orm.select(tableSpec))
        rows.sort(key=lambda row: row['team_id'])
        assert len(rows) == 2
        assert 't1' == rows[0]['team_id']
        assert 'nn1' == rows[0]['name']
        assert False == rows[0]['verified']
        assert 't2' == rows[1]['team_id']
        assert 'n2' == rows[1]['name']
        assert True == rows[1]['verified']

        orm.dropTable(tableSpec)

        #select
        #dbFile = os.path.join(tempfile.gettempdir(), str(uuid4()))
        dbFile = ':memory:'
        db = DB(dbFile)
        db.query('CREATE TABLE strvec3 (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))')
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)");
        tableSpec = {
            'name': "strvec3",
            'columnSpecs': {
                'id': { 'definition': "TEXT NOT NULL", 'transform': Ts.str, },
                'a': { 'definition': "REAL" },
                'b': { 'definition': "REAL", 'transform': Ts.str, },
                'c': { 'definition': "REAL", 'transform': Ts.str, },
            },
            'primaryKeys': ["id"]
        }
        tableSpec['related'] = [{
            'select': 'SELECT "a", Count(*) "aCount" FROM "strvec3" GROUP BY "a"',
            'foreignKeyMap': {
                'a': 'a'
            },
            'columns': {
                'aCount': 'aCount + 1000',
            }
        }]

        assert True == orm.tableExists(tableSpec)
        assert False == orm.ensureTable(tableSpec)

        qpT = orm.select(tableSpec)
        qpT = pipe.order(qpT, ['id'])
        rows = db.query(qpT, debug=1)
        #for row in rows:
        #    print(row)
        assert rows[0]['id'] == '111'
        assert rows[0]['a'] == 1
        assert rows[0]['aCount'] == 1002
        assert rows[2]['id'] == '222'
        assert rows[2]['a'] == 2
        assert rows[2]['aCount'] == 1001
        assert rows[3]['id'] == '321'
        assert rows[3]['a'] == 3
        assert rows[3]['aCount'] == 1002
        ##Pipe
        # equals
        q = 'SELECT * FROM strvec3'
        p = []
        q,p, T = pipe.equals((q, p), map=[
            {'a': 1, 'b': 1, 'c': 1},
            {'a': 3, 'b': 2, 'c': 1},
        ])
        rows = db.query((q,p, T))
        assert len(rows) == 2

        # map, pipe
        q = 'SELECT * FROM strvec3'
        p = []
        qpT = pipe.concat((q, p), pipes=[ 
            [pipe.equals, {'map': {'a': 1, 'b': 1, 'c': 1}}]
        ])
        rows = db.query((qpT))
        assert len(rows) == 1
        assert rows[0]['id'] == '111'

        # pipeLike, pipePipe
        q = 'SELECT * FROM strvec3'
        p = []
        q,p, T = pipe.concat((q, p), pipes=[ 
            [pipe.like, {'expr': 'id', 'pattern': '%2%'}],
            [pipe.order, {'exprs': ['id'], 'orders':['DESC']}],
        ])
        rows = db.query((q,p, T))
        assert len(rows) == 3
        assert rows[2]['id'] == '123'
        assert rows[1]['id'] == '222'
        assert rows[0]['id'] == '321'

        # pipeValues, order, limit
        q = 'SELECT * FROM strvec3'
        p = []
        q,p, T = pipe.concat((q, p), pipes=[ 
            [pipe.member, {'expr': 'a', 'values':['2', '3']}],
            [pipe.order, {'exprs': ['a', 'b', 'c'], 'orders':['DESC', 'DESC', 'DESC']}],
            [pipe.limit, {'limit': 10}],
        ])
        rows = db.query((q,p, T))
        assert len(rows) == 3
        assert rows[0]['id'] == '333'
        assert rows[1]['id'] == '321'
        assert rows[2]['id'] == '222'

        q = 'SELECT * FROM strvec3'
        p = []
        q,p, T = pipe.concat((q, p), [
            [pipe.member, {'expr': 'a', 'values':['2', '3']}],
            [pipe.order, {'exprs': ['a', 'b', 'c'], 'orders':['DESC', 'DESC', 'DESC']}],
            [pipe.limit, {'limit': 2, 'offset': 1}]
        ])
        #print(rows)
        rows = db.query((q,p, T))
        assert len(rows) == 1
        assert rows[0]['id'] == '222'

        # pipeOr
        q = 'SELECT * FROM strvec3'
        p = []
        q,p, T = pipe.concat((q, p, T), pipes=[
            [
                pipe.any, {
                    'pipes': [
                        [pipe.equals, {'map': {'id': '222'}}],
                        [pipe.equals, {'map': {'id': '333'}}],
                    ]
                }
            ],
            [pipe.order, {'exprs': ['id'], 'orders':['DESC']}],
        ])
        rows = db.query((q,p, T))
        assert len(rows) == 2
        assert rows[0]['id'] == '333'
        assert rows[1]['id'] == '222'

        # pipeAnd
        q = 'SELECT * FROM strvec3'
        p = []
        q,p, T = pipe.concat((q, p), pipes=[
            [
                pipe.all, {
                    'pipes': [
                        [pipe.equals, {'map': {'a': '1'}}],
                        [pipe.equals, {'map': {'c': '1'}}],
                    ]
                }
            ],
            [pipe.order, {'exprs': ['id'], 'orders':['DESC']}],
        ])
        rows = db.query((q,p, T))
        assert len(rows) == 1
        assert rows[0]['id'] == '111'


        #pipe as list, string and callable
        q = 'SELECT * FROM strvec3'
        p = []
        q,p,T = pipe.concat((q, p, None), [
            [pipe.member((q, p, T), expr= 'b', values=['2'])],
            #[pipe.member, {'expr': 'b', 'values':['2']}],
            ['order', {'exprs': ['a', 'b', 'c'], 'orders':['DESC', 'DESC', 'DESC']}],
            [pipe.limit, {'limit': 2, 'offset': 2}]
        ])
        rows = db.query((q,p,T))
        assert len(rows) == 1
        assert rows[0]['id'] == '123'

        print('dbFile={}'.format(dbFile))


        #aggregate
        db = DB(dbFile)
        db.query('CREATE TABLE strvec3 (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))')
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('113', 1, 1, 3)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)");
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)");
        q = 'SELECT * FROM strvec3'
        p = []
        q,p,T = pipe.order(
            pipe.aggregate((q, p,T), {
                'sum_a': 'sum(a)',
            }, ['id'], quote=True
            ), ['id']
        )
        rows = db.query((q,p, T))
        #print(rows)
        assert len(rows) == 6
        assert rows[0]['id'] == '111'
        assert rows[0]['sum_a'] == 1
        assert rows[5]['id'] == '333'
        assert rows[5]['sum_a'] == 3

        #DB.__DEBUG__ = True
        q = 'SELECT * FROM strvec3'
        p = []
        q,p,T = pipe.order(
            pipe.aggregate((q, p,T), {
                'sum_a': 'sum(a)',
                'sum_c': 'sum(c)',
            }, {
                'subid': "substr(id, 1 ,2)",
                'a': 'a',
            }, quote=True
            ), ['subid']
        )
        rows = db.query((q,p,T))
        #print(rows)
        assert len(rows) == 5
        assert rows[0]['subid'] == '11'
        assert rows[0]['a'] == 1
        assert rows[0]['sum_a'] == 2
        assert rows[0]['sum_c'] == 4
        assert rows[4]['subid'] == '33'
        assert rows[4]['a'] == 3
        assert rows[4]['sum_a'] == 3
        assert rows[4]['sum_c'] == 3

        print('dbFile={}'.format(dbFile))
        

if __name__ == '__main__':

    unittest.main()
