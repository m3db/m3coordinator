## WARNING: This is Alpha software and not intended for use until a stable release.

# M3Coordinator [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]

Service to access M3DB

### Docker image

> Note that all commands are run within the root of the m3coordinator directory except where specified.

You can launch a Prometheus and m3coordinator container using `docker-compose`. However, you must first build the m3coordinator Docker image.

To do so, you will need the m3coordinator binary:

    $ GOOS=linux GOARCH=amd64 CGO_ENABLED=0 make services

Once you have the binary, you can run the following to make the Docker image:

    $ docker build -t m3coordinator -f docker/Dockerfile .

Finally, you can spin up the two containers using `docker-compose` within the `docker/` directory:

    $ docker-compose up

<hr>

This project is released under the [MIT License](LICENSE.md).

[doc-img]: https://godoc.org/github.com/m3db/m3coordinator?status.svg
[doc]: https://godoc.org/github.com/m3db/m3coordinator
[ci-img]: https://travis-ci.org/m3db/m3coordinator.svg?branch=master
[ci]: https://travis-ci.org/m3db/m3coordinator
[cov-img]: https://coveralls.io/repos/github/m3db/m3coordinator/badge.svg?branch=master&service=github
[cov]: https://coveralls.io/github/m3db/m3coordinator?branch=master
