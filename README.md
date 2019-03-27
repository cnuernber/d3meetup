# d3meetup

A quick walkthrough garmin/strava data for the local d3 meetup.

A quick view of the results is available through [nbviewer](https://nbviewer.jupyter.org/github/cnuernber/d3meetup/blob/85c71625fb7813d7d1fb07f0750f4a57684f132c/walkthrough.ipynb).

# Usage

```bash
scripts/get-data.sh

pushd clojupyter
make && make install
popd
```

If you like, you can run a condensed form of the project from the repl.  Just open [doit.clj](src/d3meetup/doit.clj).

The chart interactivity isn't as nice in the repl form.  But the development experience is a hell of a lot better.

## License

Copyright © 2019 Chris Nuernberger

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
