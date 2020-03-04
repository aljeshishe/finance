# Template project

This is template for sqlachemy+pymysql+alembic projects

## Quickstart
Check everything works
    
    ./test.sh
Wait for "Everything works" message

Start database in docker
    
    ./db_start.sh
    
Create conda environment and activate it

    conda env create -f requirements.yaml -p ./env
    . activate ./env

Create database

    python db.py create

Create first database revision from model.py

    python db.py alembic revision --autogenerate -m "comment"

Do migrations

    python db.py alembic upgrade head

Check that we can add records to db

    python db.py alembic test

Drop database

    python db.py drop

