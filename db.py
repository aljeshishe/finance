# -*- coding: utf-8 -*-
import logging
import sys
from datetime import datetime

from alembic.config import main
import click
import model


@click.group()
def cli():
    pass

@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def alembic(args):
    new_args = ['-c', 'alembic/alembic.ini']
    new_args.extend(args)
    sys.exit(main(argv=new_args))


@cli.command()
def create():
    from sqlalchemy import create_engine
    engine = create_engine(model.SQLALCHEMY_SERVER_URI, echo=True)
    engine.execute("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET utf8" % model.SQLALCHEMY_DATABASE_NAME)


@cli.command()
def drop():
    from sqlalchemy import create_engine
    engine = create_engine(model.SQLALCHEMY_SERVER_URI, echo=True)
    engine.execute("DROP DATABASE IF EXISTS %s" % model.SQLALCHEMY_DATABASE_NAME)


@cli.command()
def test():
    from model import Item
    session = model.Session()
    item = Item()
    item.insert_date_time = datetime.now()
    item.eval = model.Evaluation.OVERVALUED
    item.recmd_short = model.Recomendation.BUY
    item.recmd_mid = model.Recomendation.SELL
    item.recmd_long = model.Recomendation.HOLD
    item.pattern = model.Pattern.Bearish
    item.pattern_type = 'MACD'
    item.est_return = 2
    session.add(item)
    session.commit()

    print(session.query(Item).filter_by(eval=model.Evaluation.OVERVALUED).all())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)03d|%(levelname)-4.4s|%(thread)-6.6s|%(message)s',
                        datefmt='%Y/%m/%d %H:%M:%S',
                        filename='finance.log')
    cli()
