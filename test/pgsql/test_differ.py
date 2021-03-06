import unittest
import testing.postgresql
import tempfile
from uuid import uuid4
from os import path
from contrib.p4thpy.tools import Tools
from db.pgsql.differ import Differ
from db.pgsql.differ import QueryRunner
from db.pgsql.differ import QueryFactory
from db.pgsql.util import Util
from db.pgsql.db import DB

class TestDiffer(unittest.TestCase):

    def test_QueryFactory(self):
        with testing.postgresql.Postgresql() as testpg:        
            #DB
            db = DB(url=testpg.url())
            #db = DB(username='klas', password="Hob11Nob" ,db='postgres_inlinesql')

            
            db.query('CREATE schema "u"')
            db.query('CREATE schema "b"')
            db.query('CREATE schema "p"')
            db.query('CREATE TABLE "u"."user" (id SERIAL PRIMARY KEY, name TEXT, age INTEGER)')
            db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author1\', 31)')
            db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author2\', 32)')
            db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author3\', 33)')
            db.query('CREATE TABLE "b".book (id SERIAL PRIMARY KEY, title TEXT, author TEXT)')
            db.query('INSERT INTO "b".book (title, author) VALUES (\'book1\', \'author1\')')
            db.query('INSERT INTO "b".book (title, author) VALUES (\'book2\', \'author2\')')
            db.query('INSERT INTO "b".book (title, author) VALUES (\'book3\', \'author3\')')
            db.query('CREATE TABLE "p"."player" (id SERIAL PRIMARY KEY, name TEXT, position TEXT)')
            db.query('INSERT INTO "p".player (name, position) VALUES (\'player1\', \'lw\')')
            util = Util()
            queryRunner = QueryRunner(db)
            queryFactory = QueryFactory()

            #QueryFactory: columnsQuery
            flatTableColumnRows = queryRunner.run(queryFactory.columnsQuery(tableRE='b[.]user'))
            #print(flatTableColumnRows)
            self.assertEqual(len(flatTableColumnRows), 0)

            flatTableColumnRows = queryRunner.run(queryFactory.columnsQuery(tableRE='u[.]user'))
            #print(flatTableColumnRows)
            self.assertEqual(len(flatTableColumnRows), 3)
            flatTableColumnRows = queryRunner.run(queryFactory.columnsQuery(tableRE='^u[.].*$|^b[.].*$'))
            #print(flatTableColumnRows)
            self.assertEqual(len(flatTableColumnRows), 6)
            self.assertEqual(flatTableColumnRows[0], {'table': 'b.book', 'column': 'author', 'isPrimaryKey': 0})
            assert flatTableColumnRows[1] == {'table': 'b.book', 'column': 'id', 'isPrimaryKey': 1}
            assert flatTableColumnRows[2] == {'table': 'b.book', 'column': 'title', 'isPrimaryKey': 0}
            assert flatTableColumnRows[3] == {'table': 'u.user', 'column': 'age', 'isPrimaryKey': 0}
            assert flatTableColumnRows[4] == {'table': 'u.user', 'column': 'id', 'isPrimaryKey': 1}
            assert flatTableColumnRows[5] == {'table': 'u.user', 'column': 'name', 'isPrimaryKey': 0}

            allTables = Tools.pipe(flatTableColumnRows, [
                [Tools.pluck, [lambda r, i: r['table']], {}],
                [Tools.unique, [], {}]
            ])
            #print(allTables)
            assert allTables == ['b.book', 'u.user']

    def test_QueryRunner(self):
        with testing.postgresql.Postgresql() as testpg:        
            #DB
            db = DB(url=testpg.url())
            #db = DB(username='klas', password="Hob11Nob" ,db='postgres_inlinesql')

            
            db.query('CREATE schema "u"')
            db.query('CREATE schema "b"')
            db.query('CREATE schema "p"')
            db.query('CREATE TABLE "u"."user" (id SERIAL PRIMARY KEY, name TEXT, age INTEGER)')
            db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author1\', 31)')
            db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author2\', 32)')
            db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author3\', 33)')
            db.query('CREATE TABLE "b".book (id SERIAL PRIMARY KEY, title TEXT, author TEXT)')
            db.query('INSERT INTO "b".book (title, author) VALUES (\'book1\', \'author1\')')
            db.query('INSERT INTO "b".book (title, author) VALUES (\'book2\', \'author2\')')
            db.query('INSERT INTO "b".book (title, author) VALUES (\'book3\', \'author3\')')
            db.query('CREATE TABLE "p"."player" (id SERIAL PRIMARY KEY, name TEXT, position TEXT)')
            db.query('INSERT INTO "p".player (name, position) VALUES (\'player1\', \'lw\')')
            util = Util()
            queryRunner = QueryRunner(db)
            queryFactory = QueryFactory()

            dumpPath = path.join(tempfile.gettempdir(), str(uuid4()))
            restorePath = path.join(tempfile.gettempdir(), str(uuid4()))
            db.exportToFile(dumpPath)

            with testing.postgresql.Postgresql() as restpg:
                rdb = DB(restpg.url())
                queryRunnerRestore = QueryRunner(rdb)
                rdb.exportToFile(dumpPath, invert=True);  
                booksRestore = queryRunnerRestore.run('SELECT * FROM b.book ORDER BY id');
                books = queryRunner.run('SELECT * FROM b.book ORDER BY id');
                assert booksRestore == books
                usersRestore = queryRunnerRestore.run('SELECT * FROM "u"."user" ORDER BY id');
                users = queryRunner.run('SELECT * FROM "u"."user" ORDER BY id');
                assert usersRestore == users


    def test_legacy(self):

        DB.__DEBUG__ = False


        #DBDiff
        with testing.postgresql.Postgresql() as testpg:        
            #DB
            db = DB(url=testpg.url())

            db.query('CREATE schema "u"')
            db.query('CREATE schema "b"')
            db.query('CREATE schema "p"')
            db.query('CREATE TABLE u."user" (id SERIAL PRIMARY KEY, name TEXT, age INTEGER)')
            db.query('INSERT INTO u."user" (name, age) VALUES (\'author1\', 31)')
            db.query('INSERT INTO u."user" (name, age) VALUES (\'author2\', 32)')
            db.query('INSERT INTO u."user" (name, age) VALUES (\'author3\', 33)')
            db.query('CREATE TABLE b.book (id SERIAL PRIMARY KEY, title TEXT, author TEXT)')
            db.query('INSERT INTO b.book (title, author) VALUES (\'book1\', \'author1\')')
            db.query('INSERT INTO b.book (title, author) VALUES (\'book2\', \'author2\')')
            db.query('INSERT INTO b.book (title, author) VALUES (\'book3\', \'author3\')')
            db.query('CREATE TABLE "p"."player" (id SERIAL PRIMARY KEY, name TEXT, position TEXT)')
            db.query('INSERT INTO "p".player (name, position) VALUES (\'player1\', \'lw\')')


            differ = Differ(db)


            #DBDiff: differ - no diff
            spec = {
                'tableRE': '^u[.]user$|^b[.].*$',
            }
            spec = differ.prepare(spec)
            #print(spec)

            assert 'tables' in spec
            assert len(spec['tables']) == 2

            assert 'columns' in spec
            assert len(spec['columns']) == 6
            self.assertEqual(spec['columns'][0], {'table': 'b.book', 'column': 'author', 'isPrimaryKey': 0})
            assert spec['columns'][1] == {'table': 'b.book', 'column': 'id', 'isPrimaryKey': 1}
            assert spec['columns'][2] == {'table': 'b.book', 'column': 'title', 'isPrimaryKey': 0}
            assert spec['columns'][3] == {'table': 'u.user', 'column': 'age', 'isPrimaryKey': 0}
            assert spec['columns'][4] == {'table': 'u.user', 'column': 'id', 'isPrimaryKey': 1}
            assert spec['columns'][5] == {'table': 'u.user', 'column': 'name', 'isPrimaryKey': 0}

            assert 'tableCloneMap' in spec
            assert len(spec['tableCloneMap']) == 2
            #print(spec)
            assert 'book' in spec['tableCloneMap']['b.book']
            assert 'user' in spec['tableCloneMap']['u.user']

            differ.diff(spec)
            #print(spec)
            assert 'tableDiffMap' in spec
            assert 'u.user' in spec['tableDiffMap']
            assert 'deleted' in spec['tableDiffMap']['u.user']
            assert len(spec['tableDiffMap']['u.user']['deleted']) == 0
            assert 'b.book' in spec['tableDiffMap']
            assert 'deleted' in spec['tableDiffMap']['b.book']
            assert len(spec['tableDiffMap']['b.book']['deleted']) == 0


            #DBDiff: differ - all diffs
            spec = {
                'tableRE': '^u[.]user$|^b[.].*$',
                }
            spec = differ.prepare(spec=spec)
            db.query('DELETE FROM u."user" WHERE id = 2')
            db.query('DELETE FROM u."user" WHERE id = 3')
            db.query("UPDATE u.\"user\" SET name = 'author1_1' WHERE id = 1")
            db.query('INSERT INTO u."user" (name, age) VALUES (\'author4\', 34)')

            db.query("UPDATE b.book SET title = 'book1_1' WHERE id = 1")
            db.query("UPDATE b.book SET title = 'book1_2' WHERE id = 1")
            db.query("UPDATE b.book SET title = 'book1_3' WHERE id = 1")
            db.query("UPDATE b.book SET title = 'book1_4' WHERE id = 1")
            db.query('DELETE FROM b.book WHERE id = 1')
            db.query("UPDATE b.book SET title = 'book2_2' WHERE id = 2")
            db.query("UPDATE b.book SET title = 'book3_3', author = 'author3_3' WHERE id = 3")
            db.query('INSERT INTO b.book (title, author) VALUES (\'book4\', \'author4\')')
            db.query('INSERT INTO b.book (title, author) VALUES (\'book5\', \'author5\')')

            differ.diff(spec)
            # print(spec)
            assert 'primaryKeys' in spec
            assert spec['primaryKeys'] == ['id']

            assert 'tableDiffMap' in spec
            assert 'u.user' in spec['tableDiffMap']

            assert 'deleted' in spec['tableDiffMap']['u.user']
            assert len(spec['tableDiffMap']['u.user']['deleted']) == 2
            assert spec['tableDiffMap']['u.user']['deleted'][0] == {'id': 2, 'name': 'author2', 'age': 32}
            assert spec['tableDiffMap']['u.user']['deleted'][1] == {'id': 3, 'name': 'author3', 'age': 33}

            assert 'changed' in spec['tableDiffMap']['u.user']
            assert len(spec['tableDiffMap']['u.user']['changed']) == 1
            assert spec['tableDiffMap']['u.user']['changed'][0] == {'id': 1, 'name': 'author1_1', 'age': 31}

            assert 'created' in spec['tableDiffMap']['u.user']
            assert len(spec['tableDiffMap']['u.user']['created']) == 1
            assert spec['tableDiffMap']['u.user']['created'][0] == {'id': 4, 'name': 'author4', 'age': 34}


            assert 'b.book' in spec['tableDiffMap']
            assert 'deleted' in spec['tableDiffMap']['b.book']
            assert len(spec['tableDiffMap']['b.book']['deleted']) == 1
            assert spec['tableDiffMap']['b.book']['deleted'][0] == {'id': 1, 'title': 'book1_4', 'author': 'author1'}

            assert 'changed' in spec['tableDiffMap']['b.book']
            assert len(spec['tableDiffMap']['b.book']['changed']) == 2
            assert spec['tableDiffMap']['b.book']['changed'][0] == {'id': 2, 'title': 'book2_2', 'author': 'author2' }
            assert spec['tableDiffMap']['b.book']['changed'][1] == {'id': 3, 'title': 'book3_3', 'author': 'author3_3'}

            assert 'created' in spec['tableDiffMap']['b.book']
            assert len(spec['tableDiffMap']['b.book']['created']) == 2
            assert spec['tableDiffMap']['b.book']['created'][0] == {'id': 4, 'title': 'book4', 'author': 'author4'}
            assert spec['tableDiffMap']['b.book']['created'][1] == {'id': 5, 'title': 'book5', 'author': 'author5'}

            differ.finalize(spec)


