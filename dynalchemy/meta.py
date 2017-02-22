from sqlalchemy import Column, Integer, ForeignKey, Float, String, DateTime, Time
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
            '__tablename__': table.name,
            'ID': table.id,
            'id': Column(Integer, primary_key=True)
        }
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
        print '>>>>', dir(self._base.metadata)
        print self._base.metadata.tables

    def add_column(self, collection, name, attrs):
        """ add a column to an existing table """

        table = self.get(collection, name)
        col = DColumn(table_id=table.ID, name=attrs['name'], kind=attrs['kind'])
        self._session.add(col)
        self._session.commit()
        '''table.__table__.append_column(col.to_sa())'''
        self._store_in_cache(self._session.query(DTable).get(table.ID))

    def get(self, collection, name):
        """ retrieve one table """

        return self._cache['%s.%s' % (collection, name)]

    def list(self, collection):
        """ retrieve a collection of tables """

        return [self._cache[x] for x in self._cache if x.split('.')[0] == collection]
