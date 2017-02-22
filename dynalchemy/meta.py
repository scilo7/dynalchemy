from sqlalchemy import Column, Integer, ForeignKey, Float, String, DateTime, Time
from sqlalchemy.schema import CreateColumn
from .models import DColumn, DTable


class Registry(object):
    """ storage for dynamically created classes """

    def __init__(self, base, session):

        self._base = base
        self._session = session
        self._create_meta_tables()
        self._cache = {}
        self._init_cache()


    def _store_in_cache(self, table):

        klass = self._to_sa(table)
        self._cache[table.get_key()] = klass
        return klass

    def _init_cache(self):

        for table in self._session.query(DTable).all():
            self._store_in_cache(self._to_sa(table))

    def _to_sa(self, table):

        dct = {
            '__tablename__': table.get_name(),
            '__table_args__': {'extend_existing': True},
            'ID': table.id,
            'id': Column(Integer, primary_key=True)
        }
        if table.schema:
            dct['__table_args__']['schema'] = table.schema
        for col in table.columns:
            dct[col.name] = col.to_sa()
        return type(str(table.name.capitalize()), (self._base,), dct)

    def _create_meta_tables(self):

        DTable.__table__.create(bind=self._session.get_bind())
        DColumn.__table__.create(bind=self._session.get_bind())

    def add(self, collection, name, columns=None, schema=None):
        """ add a new table """

        table = DTable(collection=collection, name=name, schema=schema)
        self._session.add(table)
        for col in columns:
            table.columns.append(DColumn(name=col['name'], kind=col['kind']))
        self._session.commit()
        klass = self._store_in_cache(table)
        klass.__table__.create(bind=self._session.get_bind())

    def add_column(self, collection, name, attrs):
        """ add a column to an existing table """

        klass = self.get(collection, name)
        col = DColumn(table_id=klass.ID, name=attrs['name'], kind=attrs['kind'])
        self._session.add(col)
        self._session.commit()
        sql = 'alter table %s add %s' % (klass.__tablename__,
            CreateColumn(col.to_sa()).compile(self._session.get_bind()))
        con = self._session.get_bind().connect()
        con.execute(sql)
        con.close()
        self._store_in_cache(self._session.query(DTable).get(klass.ID))
        return self.get(collection, name)

    def get(self, collection, name):
        """ retrieve one table """

        return self._cache['%s.%s' % (collection, name)]

    def list(self, collection):
        """ retrieve a collection of tables """

        return [self._cache[x] for x in self._cache if x.split('.')[0] == collection]
