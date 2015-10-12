# sor_ipython
To run against dse:
Make sure you run in a venv with pip dependencies installed.

To install pip dependencies:

http://docs.python-guide.org/en/latest/dev/virtualenvs/

and then run the pip installs in the pip_dependencies.txt file.

Make sure you have dse 4.8.0, and kafka running, and a topic named "test"

First start the coupons.py program:
$ python coupons.py

Once you see "adding 1 partitions", run:

$ dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 <full path to aggs.py>

You should see the output on the screen. working on saving to Cassandra for viewing.
