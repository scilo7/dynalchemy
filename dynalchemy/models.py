
import sqlalchemy

from sqlalchemy import Column, Integer, ForeignKey, String
from sqlalchemy import Boolean, PickleType, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

# this Base is distinct from the application one
# no need to mess the app with those classes
Base = declarative_base()


class DTable(Base):

    __tablename__ = 'dynalchemy_table'
    __table_args__ = (
        UniqueConstraint('collection', 'name'),
    )

    id = Column(Integer, primary_key=True)
    collection = Column(String, nullable=False)
    name = Column(String, nullable=False)
    schema = Column(String)
    active = Column(Boolean, nullable=False, default=True)

    def get_name(self):
        """ a unique name for this table """

        return '%s__%s' % (self.collection, self.name)

    def to_sa(self, registry):
        """ create mapped sa class from db definition """

        dct = {
            '__tablename__': self.get_name(),
            '__table_args__': {'extend_existing': True},
            'ID': self.id,
            'id': Column(Integer, primary_key=True)
        }
        parent_rels = {}
        if self.schema:
            dct['__table_args__']['schema'] = self.schema

        for col in self.columns:
            if col.is_many_relationship():
                continue
            dct[col.get_name()] = col.to_sa()
            if col.is_parent_relationship():
                parent_rels[col.name] = col.get_parent_relationship(registry)

        klass = type(str(self.get_name()), (registry._base,), dct)
        for key, val in parent_rels.items():
            setattr(klass, key, val)
        return klass


class DColumn(Base):
    """ Column types

        Note on relations:
        * ManyRelation:
            the kind can be anything but should be 'ManyRelation'
            as a convention. No column is created, but a relationship
        * ParentRelation:
            The kind is 'Integer'.
            The foreign key takes the name col.name + '__id'
    """

    COLUMN_TYPES = {
        # accepted types & their construction args
        'BigInteger': [],
        'Binary': [],
        'Boolean': [],
        'Date': [],
        'DateTime': [],
        'Enum': [dict(name='choices', mandatory=True)],
        'Float': [dict(name='precision', mandatory=False)],
        'Integer': [dict(name='relation', mandatory=False)],
        'LargeBinary': [dict(name='length', mandatory=False)],
        'Numeric': [],
        'SmallInteger': [],
        'String':  [dict(name='length', mandatory=False)],
        'Text':  [dict(name='length', mandatory=False)],
        'Time': []
    }

    __tablename__ = 'dynalchemy_column'

    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('dynalchemy_table.id'))
    name = Column(String, nullable=False)
    kind = Column(String, nullable=False, default='String')
    active = Column(Boolean, nullable=False, default=True)
    nullable = Column(Boolean, nullable=False, default=False)
    default = Column(String)
    length = Column(Integer)
    choices = Column(PickleType)
    precision = Column(Integer)
    relation = Column(PickleType)

    table = relationship(DTable, backref='columns') #backref('columns', lazy='joined'))

    def validate(self):
        """ Ensure attributes correctness
            Should check type/value for default, length, choices...
        """
        pass

    def get_name(self):
        """ Return name of the columm: name for std cols, name__id for
            parent relationship

            :return: column name in sa model
            :rtype: string
        """

        if self.is_relationship():
            return '%s__id' % self.name
        else:
            return self.name

    def is_relationship(self):
        """ True if the column is a relationship """

        return self.relation

    def is_parent_relationship(self):
        """ True if the column is a parent relationship """

        return self.relation and self.relation['type'] == 'parent'

    def is_many_relationship(self):
        """ True if the column is a many relationship """

        return self.relation and self.relation['type'] == 'many'

    def get_secondary_tablename(self):
        """ return the name of the secondary table in a many relationship """

        return '%s__%s__association' % (
            self.table.name, self.relation['name'])

    def get_remote(self, registry):
        """ return the remote SA model in a relationship """

        return registry.get(self.relation['collection'], self.relation['name'])

    def get_secondary(self, registry):
        """ return the secondary SA model in a many relationship """

        return registry.get(
            self.table.collection,
            self.get_secondary_tablename())

    def get_parent_relationship(self, registry):
        """ return the SA model in a parent relationship """

        bref_name = self.relation.get('backref',
            '%s_collection' % self.table.name)

        return relationship(
            self.get_remote(registry),
            cascade="save-update, merge",
            backref=backref(bref_name, cascade="all, delete-orphan")
        )

    def get_many_relationship(self, registry):
        """ return the SA relationship in a many relationship """

        bref_name = self.relation.get('backref',
            '%s_collection' % self.table.name)

        return relationship(
            self.get_remote(registry),
            secondary=self.get_secondary(registry).__table__,
            cascade="save-update, merge",
            backref=backref(bref_name)
        )

    def _get_type(self):
        """ sqlalchemy type for the column """

        kind = getattr(sqlalchemy, self.kind)
        if self.kind == 'String' and self.length:
            kind = kind(self.length)
        if self.kind == 'Enum':
            kind = kind(*self.choices)
        return kind

    def _get_default(self):
        """ convert default to the correct type """

        if self.kind in ('BigInteger', 'Integer', 'SmallInteger'):
            return int(self.default)
        elif self.kind in ('Float', 'Numeric'):
            return float(self.default)
        elif self.kind == 'Boolean':
            if self.default in ('t', 'true', 'y', 'on', '1'):
                return True
            else:
                return False
        else:
            return self.default

    def _get_args(self):
        """ sqlalchmey extra args for column creation """

        args = {}
        if self.default is not None:
            args['default'] = self._get_default()
        if self.kind in ('Numeric', 'Float') and self.precision:
            args['precision'] = self.precision
        return args

    def to_sa(self):
        """ Convert to sa object Column
            Many relationships are not created yet as the secondary table does
            not exists yet
        """

        args = self._get_args()
        kind = self._get_type()
        if self.is_parent_relationship():
            fkey = '%(collection)s__%(name)s.id' % self.relation
            return Column(self.get_name(), kind, ForeignKey(fkey), **args)
        else:
            return Column(self.get_name(), kind, **args)

    @classmethod
    def get_serialization_fields(cls, kind):
        """ List fields availables for Column declaration of one type """

        fields = [
            dict(name='name', nullable=False),
            dict(name='kind', nullable=False),
            dict(name='nullable', nullable=True),
            dict(name='default', nullable=True),
        ]

        fields += DColumn.COLUMN_TYPES[kind]
        return fields

    def serialize(self):
        """ Convert to basic dict for json """

        fields = DColumn.get_serialization_fields(self.kind)
        for field in fields:
            field['value'] = getattr(self, field['name'])
        return fields
