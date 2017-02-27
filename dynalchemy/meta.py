from sqlalchemy import Column, Integer, ForeignKey, Float, String, DateTime, Time
from sqlalchemy.schema import CreateColumn
from sqlalchemy.ext.declarative.base import _add_attribute
from .models import DColumn, DTable


class Registry(object):
    """ storage for dynamically created classes """

    def __init__(self, base, session):

        self._base = base
        self._session = session
        self._create_meta_tables()
        self._created = []

    def _create(self, table):

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
        klass = type(str(table.get_name()), (self._base,), dct)
        return klass

    def _init_cache(self):
        """ load all tables from db and create declarative classes """

        for table in self._session.query(DTable).all():
            self._create(table)

    def _create_meta_tables(self):
        """ Create registry tables in DB """

        DTable.__table__.create(bind=self._session.get_bind())
        DColumn.__table__.create(bind=self._session.get_bind())

    def add(self, collection, name, columns=None, schema=None):
        """ add a new table """

        table = DTable(collection=collection, name=name, schema=schema)
        self._session.add(table)
        for col in columns:
            table.columns.append(DColumn(name=col['name'], kind=col['kind']))
        self._session.commit()

        klass = self._create(table)
        klass.__table__.create(bind=self._session.get_bind())
        return klass

    def add_column(self, collection, name, attrs):
        """ add a column to an existing table """

        klass = self.get(collection, name)
        col = DColumn(table_id=klass.ID, name=attrs['name'], kind=attrs['kind'])
        self._session.add(col)
        self._session.commit()
        sql = 'alter table %s add %s' % (
            klass.__tablename__,
            CreateColumn(col.to_sa()).compile(self._session.get_bind()))
        con = self._session.get_bind().connect()
        con.execute(sql)
        con.close()
        _add_attribute(klass, name, col.to_sa())

    def deprecate_column(self, collection, name, colname):
        """ Mark column colname as deprecated
            Data are not removed from database
            Class definition is updated in the Registry

            :param collection: collection name - String
            :param name: table name - String
            :param colname: column name - String
            :return: None
        """

        col = self._session.query(DColumn).join(DTable)\
            .filter(DTable.collection == collection)\
            .filter(DTable.name == name)\
            .filter(DColumn.name == colname).one()
        col.active = False
        self._session.commit()

    def _get_dtable(self, collection, name):
        
        return self._session.query(DTable)\
            .filter_by(collection=collection, name=name).one()

    def deprecate(self, collection, name):
        """ Mark table as deprecated
            Data are not removed from database
            Class definition is removed from the Registry

            :param collection: collection name - String
            :param name: table name - String
            :return: None
        """

        table = self._get_dtable(collection, name)
        table.active = False
        self._session.commit()
        self._base.metadata.remove(table)

    def get(self, collection, name, dtable=None):
        """ Retrieve one table

            :param collection: collection name - String
            :param name: table name - String
            :param dtable: dtable instance already loaded
            :return: sqlalchemy table class
        """

        key = '%s__%s' % (collection, name)
        try:
            return self._base._decl_class_registry[key]
        except:
            return self._create(dtable or self._get_dtable(collection, name))

    def list(self, collection):
        """ Retrieve a collection of tables

            :param collection: collection name - String
            :return: list of sqlalchemy table classes
        """

        all = self._session.query(DTable).filter_by(collection=collection).all()
        return [self.get(collection, dtable.name, dtable) for dtable in all]
