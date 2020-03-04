# remove all from previous runs
ENV_NAME=tenv
docker rm -f mysql
rm -rf $ENV_NAME

set -ex

docker run --name mysql -d  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_ROOT_HOST=% -p 3306:3306 -it mysql:8
sleep 10 # wait for mysql to initialize

conda env create -p $ENV_NAME -f requirements.yaml
. activate ./$ENV_NAME

python db.py create
rm -rf alembic/test_versions # there should be no migrations
python db.py alembic revision --version-path alembic/test_versions --autogenerate -m "comment"
python db.py alembic upgrade head
python db.py test
python db.py drop

# cleanup
conda deactivate
rm -rf $ENV_NAME
docker rm -f mysql

echo 'Everything works'