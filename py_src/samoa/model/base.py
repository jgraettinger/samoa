
import sqlalchemy.ext.declarative
import sqlalchemy.types

import samoa.core

# base class for all mapped table entity types
ModelBase = sqlalchemy.ext.declarative.declarative_base()

class UUIDType(sqlalchemy.types.TypeDecorator):

    impl = sqlalchemy.types.String

    def process_bind_param(self, value, dialect):
        return value.to_hex_str()

    def process_result_value(self, value, dialect):
        return samoa.core.UUID.from_hex_str(value)
