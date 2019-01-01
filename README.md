Streamy-db   ![](https://travis-ci.com/domsj/streamy-db.svg?branch=master)
--

Streamy-db is a deterministic streaming database implementation.
It should (in theory) be scalable and low latency.

It adapts [Calvin](http://cs-www.cs.yale.edu/homes/dna/papers/calvin-sigmod12.pdf) onto a stream processing framework.

Currently there is scala code for both [flink](https://flink.apache.org/) and [beam](https://beam.apache.org/) (using [scio](https://github.com/spotify/scio)).

This is only a prototype / proof of concept.

The flink code is a bit further along than the beam code.

A blog post explaining the contents of the repo is [available](https://domsj.info/2018/12/30/introducing-streamy-db.html).

As you can probably tell, I'm not used to organising scala code bases.
So I hereby do a warm request towards people to improve on that.
