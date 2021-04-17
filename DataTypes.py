#
# Copyright 2016 Jonathan Schultz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


try:
    import sqlalchemy_sqlany
    sqlany = True
except:
    sqlany = False

from sqlalchemy.databases import mssql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy import TypeDecorator, BINARY, TEXT, String
import uuid

class UUID(TypeDecorator):
    """Platform-independent UUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(36), storing as stringified hex values.

    """
    #impl = CHAR
    #http://stackoverflow.com/questions/5849389/storing-uuids-in-sqlite-using-pylons-and-sqlalchemy
    impl = BINARY

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return str(uuid.UUID(value)).upper()
            else:
                return str(value).upper()

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif not isinstance(value, uuid.UUID):
            return uuid.UUID(value)
        else:
            return value

@compiles(UUID, 'sqlite')
def compile_UUID_mssql_sqlite(element, compiler, **kw):
    """ SQLite doesn't care too much about type names, UNIQUEIDENTIFIER is fine. """
    return 'UNIQUEIDENTIFIER'

mssql.ischema_names['xml'] = String
mssql.ischema_names['uniqueidentifier'] = UUID

sqlite.ischema_names['UNIQUEIDENTIFIER'] = UUID
sqlite.ischema_names['UUIDTEXT'] = UUID

if sqlany:
    sqlalchemy_sqlany.dialect.ischema_names['xml'] = String
    sqlalchemy_sqlany.dialect.ischema_names['long nvarchar'] = TEXT
    sqlalchemy_sqlany.dialect.ischema_names['uniqueidentifier'] = UUID
