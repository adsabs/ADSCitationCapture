# ADSCitationCapture

Copy ```config.py``` to ```local_config.py``` and modify its content to reflect your system.

The project incorporates Alembic to manage PostgreSQL migrations based on SQLAlchemy models. It has already been initialized with:

```
alembic init alembic
```

And the file ```alembic/env.py``` was modified to import SQLAlchemy models and use the database connection coming from the ```local_config.py``` file. As a reminder, some key commands from [Alembic tutorial](http://alembic.zzzcomputing.com/en/latest/):

```
alembic revision -m "baseline" --autogenerate
alembic current
alembic history
alembic upgrade head
alembic downgrade -1
alembic upgrade +1
```

Some key commands to inspect the database from [the PostgreSQL cheatsheet](https://gist.github.com/Kartones/dd3ff5ec5ea238d4c546):

```
sudo apt-get install postgresql-client --no-install-recommends
psql -h localhost database user
\dt public.*
\d public.alembic_version
```


```
py.test
```
