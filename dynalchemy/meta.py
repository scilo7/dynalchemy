from sqlalchemy.schema import CreateColumn
from sqlalchemy.ext.declarative.base import _add_attribute
from .models import DColumn, DTable


class TableExistException(Exception):
    pass


class Registry(object):
    """ storage for dynamically created classes """

    def __init__(self, base, session):

        self._base = base
        self.session = session
        self._create_meta_tables()

    def _create_meta_tables(self):
        """ Create registry tables in DB """

        bind = self.session.get_bind()
        if not DTable.__table__.exists(bind):
            DTable.__table__.create(bind)
            DColumn.__table__.create(bind)

    def destroy(self):
        """ BEWARE !! - for unit tests mainly """

        self.session.close()
        self._base.metadata.drop_all()
        DColumn.__table__.drop(bind=self.session.get_bind())
        DTable.__table__.drop(bind=self.session.get_bind())

    def add(self, collection, name, columns=None, schema=None):
        """ Add a new table:
            - insert definitions in DTable & DColumn
            - Create sql table
            - Create sqlalchemy model with relationships

            :param collection: collection name, string
            :param name: table name, string
            :param columns: dictionary of columns definitions
            :param schema: optional db schema, string
            :return: sqlalchemy model
        """

        try:
            old = self.session.query(DTable).filter_by(
                collection=collection, name=name, schema=schema).one()
        except:
            # not found: ok
            pass
        else:
            raise TableExistException('table %s already defined' % name)

        table = DTable(collection=collection, name=name, schema=schema)
        self.session.add(table)
        many_relations = []
        if columns:
            for col_attrs in columns:
                dcol = DColumn(**col_attrs)
                dcol.validate()
                table.columns.append(dcol)
                if dcol.is_many_relationship():
                    many_relations.append(dcol)
        self.session.commit()

        klass = table.to_sa(self)
        klass.__table__.create(bind=self.session.get_bind())

        # secondary tables and attributes must be created afterwards
        for mrel in many_relations:
            self._add_relation_table(mrel)
            setattr(klass, dcol.name, dcol.get_many_relationship(self))
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

        # register in db
        klass = self.get(collection, name)
        col = DColumn(table_id=klass.ID, **attrs)
        col.validate()
        self.session.add(col)
        self.session.commit()

        if col.is_many_relationship():
            self._add_relation_table(col)
        else:
            # alter table
            sql = 'alter table %s add %s' % (
                col.table.get_name(),
                CreateColumn(col.to_sa()).compile(self.session.get_bind()))
            con = self.session.get_bind().connect()
            con.execute(sql)
            con.close()

        # force model to refresh: remove from declarative registry
        try:
            del self._base._decl_class_registry[col.table.get_name()]
        except:
            pass

    def _add_relation_table(self, dcol):
        """ Create secondary table in db """

        columns = [
            dict(
                name=dcol.table.name,
                kind='Integer',
                relation={
                    'collection': dcol.table.collection,
                    'name': dcol.table.name,
                    'type': 'parent'}
            ),
            dict(
                name=dcol.relation['name'],
                kind='Integer',
                relation={
                    'collection': dcol.relation['collection'],
                    'name': dcol.relation['name'],
                    'type': 'parent'}
            )
        ]

        self.add(dcol.table.collection, dcol.get_secondary_tablename(),
                 columns=columns)

    def deprecate_column(self, collection, name, colname):
        """ Mark column colname as deprecated
            Data are not removed from database
            Class definition is updated in the db

            :param collection: collection name - String
            :param name: table name - String
            :param colname: column name - String
            :return: None
        """

        col = self.session.query(DColumn).join(DTable)\
            .filter(DTable.collection == collection)\
            .filter(DTable.name == name)\
            .filter(DColumn.name == colname).one()
        col.active = False
        self.session.commit()

        try:
            # remove from declarative registry
            del self._base._decl_class_registry[col.table.get_name()]
        except:
            pass

    def _get_dtable(self, collection, name):
        """ select dtable in db """

        return self.session.query(DTable).filter_by(
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
        self.session.commit()
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
            table = dtable or self._get_dtable(collection, name)
            klass = table.to_sa(self)
            for col in table.columns:
                if col.is_many_relationship():
                    setattr(klass, col.name, col.get_many_relationship(self))
            return klass


    def list(self, collection):
        """ Retrieve a collection of tables

            :param collection: collection name - String
            :return: list of sqlalchemy table classes
        """

        all_tables = self.session.query(DTable).filter_by(
            collection=collection, active=True).all()
        return [self.get(collection, dtable.name, dtable) for dtable in all_tables]

    def get_all(self):
        return [self.get(None, None, dtable)
            for dtable in self.session.query(DTable).all()]

