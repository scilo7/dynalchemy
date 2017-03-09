from sqlalchemy import Column, Integer, ForeignKey, Float, String, DateTime, Time
from sqlalchemy.schema import CreateColumn
from sqlalchemy.ext.declarative.base import _add_attribute
from .models import DColumn, DTable


class TableExistException(Exception):
    pass


class Registry(object):
    """ storage for dynamically created classes """

    def __init__(self, base, session):

        self._base = base
        self._session = session
        self._create_meta_tables()

    def _create_meta_tables(self):
        """ Create registry tables in DB """

        DTable.__table__.create(bind=self._session.get_bind())
        DColumn.__table__.create(bind=self._session.get_bind())

    def add(self, collection, name, columns=None, schema=None):
        """ add a new table """

        try:
            old = self._session.query(DTable).filter_by(
                collection=collection, name=name, schema=schema).one()
            if old:
                raise TableExistException('table %s already defined' % name)
        except:
            # not found: ok
            pass
        table = DTable(collection=collection, name=name, schema=schema)
        self._session.add(table)
        if columns:
            for col in columns:
                col = DColumn(name=col['name'], kind=col['kind'])
                col.validate()
                table.columns.append(col)
        self._session.commit()

        klass = table.to_sa(self)
        klass.__table__.create(bind=self._session.get_bind())
        return klass

    def add_from_config(self, config):
        """ Utility to add from parameters passed in a dict
            :param config: dict containing at least collection,
                name and columns keys
        """
        for key in ('collection', 'name'):
            assert key in config, 'missing config "{}"'.format(key)
        return self.add(
            config['collection'], config['name'],
            columns=config.get('columns', []),
            schema=config.get('schema', None))

    def add_column(self, collection, name, attrs):
        """ Add a column to an existing table:
            - insert it in db (DCcolumn)
            - add column to the dynamic table in db
            - append the attribute to the base class
        """

        klass = self.get(collection, name)
        col = DColumn(table_id=klass.ID, name=attrs['name'], kind=attrs['kind'])
        col.validate()
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
            Class definition is updated in the db

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

        try:
            # remove from declarative registry
            del self._base._decl_class_registry[col.table.get_name()]
        except:
            pass

    def _get_dtable(self, collection, name):
        """ select dtable in db """

        return self._session.query(DTable).filter_by(
            collection=collection, name=name, active=True).one()

    def deprecate(self, collection, name):
        """ Mark table as deprecated
            Data are not removed from database
            Class definition is removed from metadata

            :param collection: collection name - String
            :param name: table name - String
            :return: None
        """

        table = self._get_dtable(collection, name)
        table.active = False
        self._session.commit()
        self._base.metadata.remove(table)
        try:
            del self._base._decl_class_registry[table.get_name()]
        except:
            pass

    def get(self, collection, name, dtable=None):
        """ Retrieve one mapped sqlalchemy class

            :param collection: collection name - String
            :param name: table name - String
            :param dtable: dtable instance already loaded
            :return: sqlalchemy table class
        """

        key = '%s__%s' % (collection, name)
        try:
            return self._base._decl_class_registry[key]
        except KeyError:
            return self._create(dtable or self._get_dtable(collection, name))

    def list(self, collection):
        """ Retrieve a collection of tables

            :param collection: collection name - String
            :return: list of sqlalchemy table classes
        """

        all_tables = self._session.query(DTable).filter_by(
            collection=collection, active=True).all()
        return [self.get(collection, dtable.name, dtable) for dtable in all_tables]
