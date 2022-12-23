# DAX

DAX encapsulates anything which covers all of the services which make up the
"disaggregation of storage and compute" project. Initially, this will include
integration tests which pull in things like Metadata Services (MDS), including
the Controller, as well as FeatureBase and IDK-based ingesters.

## Setting up the tests

The DAX test currently requires docker images for: `featurebase`  and `datagen`.

If at any point you run into problems with go mod failing to reference a private
repo, make sure that you have `gitlab.com/molecula` in your `GOPRIVATE`
environment variable.

Note that during the docker image build step, `go mod vendor` is run, which
creates a `vendor` directory in the root directory, and copies that to docker
during the build stage. Just be aware of this; you may want to remove that
vendor directory after you're done building docker images.

#### Possible Configuruation
The following were relevent when the dax code was in a separate repository.
These may no longer be relevant.

I needed to but this in my `~/.profile` file:

```export GOPRIVATE=github.com/molecula,gitlab.com/molecula```

And this in my `~/.gitconfig`

```
[url "ssh://git@github.com/"]
	insteadOf = https://github.com/
[url "ssh://git@gitlab.com/"]
	insteadOf = https://gitlab.com/
```

Then `make docker` ran successfully.


### Build the FeatureBase docker image

- Check out the
  [dax](https://github.com/FeatureBaseDB/faturebase/tree/dax)
  branch of the
  [featurebase](https://github.com/FeatureBaseDB/faturebase) repository.
- Run `make docker-image-featurebase` to build the docker image
- You should now have an image in docker named `dax/featurebase` with the tag `latest`.

### Build the Datagen docker image

- `cd <featurebase_repo_root>/idk`
- Run `make docker-image-datagen` to build the docker image
- You should now have an image in docker named `dax/datagen` with the tag
  `latest`.

## Running the tests

- Check out the
  [dax](https://github.com/FeatureBaseDB/faturebase/tree/dax)
  branch of the
  [featurebase](https://github.com/FeatureBaseDB/faturebase) repository.
- Change into the `dax` directory: `cd dax`
- Run `make test-integration`.
