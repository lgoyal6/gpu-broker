"""The browser side of the broker.

Deliberately powerless. This process holds no AWS credentials and no SSH key: it
reads and writes the queue, and a separate `gpu run` daemon does everything that
touches a machine. It has to be reachable from the internet for GitHub's OAuth
callback, and a reachable box holding the club's AWS keys is a bad trade for a
nicer deploy story.

So a cancel from a web page is a *request*, recorded here and acted on by the
daemon within a tick. Everything else here is a read.
"""
