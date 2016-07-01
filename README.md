# nvivotools
A range of tools to help you get more out of NVivo(tm)

The python scripts NormaliseNVP and DenormaliseNVP do much as their names imply. They take two arguments in sqlalchemy format (eg sqlite:///filename.db or mssql+pymssql://user:password@sqlservername/database) and convert the former to the latter.

They should both work equally well with python2.x or python3.x, but do require the following modules to be installed (use pip): future, pymssql, sqlalchemy.
