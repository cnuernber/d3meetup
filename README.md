# d3meetup

A quick walkthrough garmin/strava data for the local d3 meetup.

A quick view of the results is available through [nbviewer](https://nbviewer.jupyter.org/github/cnuernber/d3meetup/blob/3b9e33d6e1b622983bef9a0a9983c868940b378f/walkthrough.ipynb).

# Usage

```bash
git submodule update --recursive --init


scripts/get-data.sh

pushd clojupyter
make && make install
popd

jupyter-notebook
```

If you like, you can run a condensed form of the project from the repl.  Just open [doit.clj](src/d3meetup/doit.clj).

The chart interactivity isn't as nice in the repl form.  But the development experience is a hell of a lot better.

## License

Copyright © 2019 Chris Nuernberger

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
