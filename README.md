# Lambda GraphQL Demo

First build node_modules and required shared libraries:
```sh
docker build .
docker run $(docker build . -q) zip -r9 - lib/ node_modules/ > lib.zip
```

Ideally this would become a layer, but for now just add the application code:
```sh
cp lib.zip app.zip
zip -r9 app.zip app.mjs index.js package-lock.json package.json profiles.json
```
