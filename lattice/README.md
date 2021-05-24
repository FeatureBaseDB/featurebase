## Connecting to a standalone Pilosa instance

Lattice can run independently (with e.g. `yarn start`; see below), connecting to any Pilosa instance. Pilosa must be started with the `allowed-origins` configuration parameter set to communicate with the UI.

CLI: `pilosa server --handler.allowed-origins http://localhost:3000`

You will need to update the `lattice.config.js` file and set the hostname and port of your pilosa server, otherwise it will use the browser url. The default pilosa server is `localhost:10101`

If you will be consistently working in the repo, it may be useful to locally ignore changes to this file to prevent accidental commits by using:
 `git update-index --assume-unchanged src/lattice.config.js`

### `yarn start`

Runs the app in the development mode.<br />
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.


## Embedding Lattice within Pilosa

Lattice can be embedded within the Pilosa binary, so the UI is fully accessible directly from the server, reducing operational complexity.

If additional build dependencies `yarn` (`brew install yarn` and `brew upgrade yarn` perhaps) and `statik` (`make install-statik`) are available on your system, running `make generate-statik` before `make install` should produce a Pilosa binary with Lattice embedded. For up to date instructions, check the Pilosa [README](https://github.com/molecula/pilosa#getting-started).
