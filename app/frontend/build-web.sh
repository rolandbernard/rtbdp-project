# This is a small script that will build the frontend web code and copy it to
# the correct location in the frontend servers code.

cd ./web
npm install
npm run build

cd ..
rm -r ./src/main/resources/com/rolandb/static
cp -r ./web/dist ./src/main/resources/com/rolandb/static

