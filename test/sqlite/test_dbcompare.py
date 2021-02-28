import unittest

class TestDBCompare(unittest.TestCase):

    def test_legacy(self):
        import tempfile
        from uuid import uuid4
        from os import path
        from contrib.p4thpy.tools import Tools
        from db.sqlite.dbcompare import DBCompare
        from db.sqlite.dbcompare import QueryRunner
        from db.sqlite.dbcompare import QueryFactory
        from db.sqlite.db import DB

        DB.__DEBUG__ = False

        dbFile = ':memory:'
        db = DB(dbFile, attaches={
            #'tmp': tmpSchemaFile,
        })

        db.query('CREATE TABLE user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)')
        db.query('INSERT INTO user (name, age) VALUES ("author1", 31)')
        db.query('INSERT INTO user (name, age) VALUES ("author2", 32)')
        db.query('INSERT INTO user (name, age) VALUES ("author3", 33)')
        db.query('CREATE TABLE book (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author TEXT)')
        db.query('INSERT INTO book (title, author) VALUES ("book1", "author1")')
        db.query('INSERT INTO book (title, author) VALUES ("book2", "author2")')
        db.query('INSERT INTO book (title, author) VALUES ("book3", "author3")')

        queryRunner = QueryRunner(db);
        queryFactory = QueryFactory();

        #SqliteQueryFactory: columnsQuery
        flatTableColumnRows = queryRunner.run(queryFactory.columnsQuery())
        #print(flatTableColumnRows)
        assert len(flatTableColumnRows) == 6
        assert flatTableColumnRows[0] == {'table': 'book', 'column': 'author', 'isPrimaryKey': 0}
        assert flatTableColumnRows[1] == {'table': 'book', 'column': 'id', 'isPrimaryKey': 1}
        assert flatTableColumnRows[2] == {'table': 'book', 'column': 'title', 'isPrimaryKey': 0}
        assert flatTableColumnRows[3] == {'table': 'user', 'column': 'age', 'isPrimaryKey': 0}
        assert flatTableColumnRows[4] == {'table': 'user', 'column': 'id', 'isPrimaryKey': 1}
        assert flatTableColumnRows[5] == {'table': 'user', 'column': 'name', 'isPrimaryKey': 0}

        allTables = Tools.pipe(flatTableColumnRows, [
            [Tools.pluck, [lambda r, i: r['table']], {}],
            [Tools.unique, [], {}]
        ])
        #print(allTables)
        assert allTables == ['book', 'user']

        #SqliteQueryRunner: exportToFile
        dumpPath = path.join(tempfile.gettempdir(), str(uuid4()))
        restorePath = path.join(tempfile.gettempdir(), str(uuid4()))
        db.exportToFile(dumpPath)
        restoreDB = DB(restorePath)
        queryRunnerRestore = QueryRunner(restoreDB)
        restoreDB.exportToFile(dumpPath, invert=True);  
        booksRestore = queryRunnerRestore.run('SELECT * FROM book ORDER BY id');
        books = queryRunner.run('SELECT * FROM book ORDER BY id');
        assert booksRestore == books
        usersRestore = queryRunnerRestore.run('SELECT * FROM user ORDER BY id');
        users = queryRunner.run('SELECT * FROM user ORDER BY id');
        assert usersRestore == users


        ##SqliteQueryFactory: cloneTables
        #tables = ['book', 'user']
        #qps = queryFactory.cloneTables(tables)
        #nameMap = dict(zip(tables, qps.keys()))
        ##print(nameMap)
        #queryRunner.run(qps)
        #booksClone = queryRunner.run('SELECT * FROM "{}" ORDER BY id'
        #                             .format(nameMap['book']));
        #books = queryRunner.run('SELECT * FROM book ORDER BY id');
        #assert booksClone == books
        #usersClone = queryRunner.run('SELECT * FROM "{}" ORDER BY id'
        #                             .format(nameMap['user']));
        #users = queryRunner.run('SELECT * FROM user ORDER BY id');
        #assert usersClone == users

        #DBDiff
        dbFile = ':memory:'
        db = DB(dbFile, attaches={
            #'tmp': tmpSchemaFile,
        })

        db.query('CREATE TABLE user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)')
        db.query('INSERT INTO user (name, age) VALUES ("author1", 31)')
        db.query('INSERT INTO user (name, age) VALUES ("author2", 32)')
        db.query('INSERT INTO user (name, age) VALUES ("author3", 33)')
        db.query('CREATE TABLE book (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author TEXT)')
        db.query('INSERT INTO book (title, author) VALUES ("book1", "author1")')
        db.query('INSERT INTO book (title, author) VALUES ("book2", "author2")')
        db.query('INSERT INTO book (title, author) VALUES ("book3", "author3")')


        #queryRunner = QueryRunner(db);
        #queryFactory = QueryFactory(db);
        dbCompare = DBCompare(db)


        #DBDiff: compare - no diff
        spec = dbCompare.prepare()
        #print(spec)

        assert 'tables' in spec
        assert len(spec['tables']) == 2

        assert 'columns' in spec
        assert len(spec['columns']) == 6
        assert spec['columns'][0] == {'table': 'book', 'column': 'author', 'isPrimaryKey': 0}
        assert spec['columns'][1] == {'table': 'book', 'column': 'id', 'isPrimaryKey': 1}
        assert spec['columns'][2] == {'table': 'book', 'column': 'title', 'isPrimaryKey': 0}
        assert spec['columns'][3] == {'table': 'user', 'column': 'age', 'isPrimaryKey': 0}
        assert spec['columns'][4] == {'table': 'user', 'column': 'id', 'isPrimaryKey': 1}
        assert spec['columns'][5] == {'table': 'user', 'column': 'name', 'isPrimaryKey': 0}

        assert 'tableCloneMap' in spec
        assert len(spec['tableCloneMap']) == 2
        assert 'book' in spec['tableCloneMap']['book']
        assert 'user' in spec['tableCloneMap']['user']

        dbCompare.diff(spec)
        #print(spec)
        assert 'tableDiffMap' in spec
        assert 'user' in spec['tableDiffMap']
        assert 'deleted' in spec['tableDiffMap']['user']
        assert len(spec['tableDiffMap']['user']['deleted']) == 0
        assert 'book' in spec['tableDiffMap']
        assert 'deleted' in spec['tableDiffMap']['book']
        assert len(spec['tableDiffMap']['book']['deleted']) == 0


        #DBDiff: compare - all diffs
        spec = dbCompare.prepare(spec={})
        db.query('DELETE FROM user WHERE id = 2')
        db.query('DELETE FROM user WHERE id = 3')
        db.query("UPDATE user SET name = 'author1_1' WHERE id = 1")
        db.query('INSERT INTO user (name, age) VALUES ("author4", 34)')

        db.query('DELETE FROM book WHERE id = 1')
        db.query("UPDATE book SET title = 'book2_2' WHERE id = 2")
        db.query("UPDATE book SET title = 'book3_3', author = 'author3_3' WHERE id = 3")
        db.query('INSERT INTO book (title, author) VALUES ("book4", "author4")')
        db.query('INSERT INTO book (title, author) VALUES ("book5", "author5")')

        dbCompare.diff(spec)
        #print(spec)
        assert 'primaryKeys' in spec
        assert spec['primaryKeys'] == ['id']

        assert 'tableDiffMap' in spec
        assert 'user' in spec['tableDiffMap']

        assert 'deleted' in spec['tableDiffMap']['user']
        assert len(spec['tableDiffMap']['user']['deleted']) == 2
        assert spec['tableDiffMap']['user']['deleted'][0] == {'id': 2, 'name': 'author2', 'age': 32}
        assert spec['tableDiffMap']['user']['deleted'][1] == {'id': 3, 'name': 'author3', 'age': 33}

        assert 'changed' in spec['tableDiffMap']['user']
        assert len(spec['tableDiffMap']['user']['changed']) == 1
        assert spec['tableDiffMap']['user']['changed'][0] == {'id': 1, 'name': 'author1_1', 'age': 31}

        assert 'created' in spec['tableDiffMap']['user']
        assert len(spec['tableDiffMap']['user']['created']) == 1
        assert spec['tableDiffMap']['user']['created'][0] == {'id': 4, 'name': 'author4', 'age': 34}


        assert 'book' in spec['tableDiffMap']
        assert 'deleted' in spec['tableDiffMap']['book']
        assert len(spec['tableDiffMap']['book']['deleted']) == 1
        assert spec['tableDiffMap']['book']['deleted'][0] == {'id': 1, 'title': 'book1', 'author': 'author1'}

        assert 'changed' in spec['tableDiffMap']['book']
        assert len(spec['tableDiffMap']['book']['changed']) == 2
        assert spec['tableDiffMap']['book']['changed'][0] == {'id': 2, 'title': 'book2_2', 'author': 'author2' }
        assert spec['tableDiffMap']['book']['changed'][1] == {'id': 3, 'title': 'book3_3', 'author': 'author3_3'}

        assert 'created' in spec['tableDiffMap']['book']
        assert len(spec['tableDiffMap']['book']['created']) == 2
        assert spec['tableDiffMap']['book']['created'][0] == {'id': 4, 'title': 'book4', 'author': 'author4'}
        assert spec['tableDiffMap']['book']['created'][1] == {'id': 5, 'title': 'book5', 'author': 'author5'}


if __name__ == '__main__':
    unittest.main()